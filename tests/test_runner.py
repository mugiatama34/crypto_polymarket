import asyncio
import json
import subprocess
from datetime import datetime, timezone

import httpx
import pytest

from collector.clock import FakeClock
from collector.runner import LongjobRunner

ALIGNED_EPOCH_S = 1717000200  # 300s-hizali


def _git(args, cwd):
    result = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    return result


def _init_repo(tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    _git(["init", "-b", "main"], cwd=repo_dir)
    _git(["config", "user.email", "test@example.com"], cwd=repo_dir)
    _git(["config", "user.name", "Test"], cwd=repo_dir)
    (repo_dir / "README.md").write_text("init\n", encoding="utf-8")
    _git(["add", "README.md"], cwd=repo_dir)
    _git(["commit", "-m", "init"], cwd=repo_dir)
    return repo_dir


class FakeSimpleWSClient:
    """RTDS icin: run()/stop() ile calisir, cache statik verilir."""

    def __init__(self, cache):
        self.cache = cache
        self._stopped = False
        self.ran = False

    def snapshot(self, key):
        return self.cache.get(key)

    async def run(self):
        self.ran = True
        while not self._stopped:
            await asyncio.sleep(0)

    def stop(self):
        self._stopped = True


class FakeClobWSClient(FakeSimpleWSClient):
    def __init__(self, cache):
        super().__init__(cache)
        self.subscribe_calls = []

    async def subscribe(self, asset_ids):
        self.subscribe_calls.append(list(asset_ids))


def _book_side():
    return {
        "best_bid": 0.48,
        "best_ask": 0.52,
        "bid_size": 10.0,
        "ask_size": 8.0,
        "spread": 0.04,
        "mid": 0.5,
        "bids_top5": [[0.48, 10.0]],
        "asks_top5": [[0.52, 8.0]],
    }


def _make_http_client(requested_slugs):
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "/events" in url:
            slug = request.url.params.get("slug")
            requested_slugs.append(slug)
            return httpx.Response(
                200,
                json=[
                    {
                        "id": "evt-1",
                        "slug": slug,
                        "conditionId": "0xabc",
                        "outcomes": json.dumps(["Up", "Down"]),
                        "clobTokenIds": json.dumps(["111", "222"]),
                    }
                ],
            )
        if "/book" in url:
            return httpx.Response(
                200,
                json={
                    "timestamp": "1717000060000",
                    "bids": [{"price": "0.48", "size": "10"}],
                    "asks": [{"price": "0.52", "size": "8"}],
                },
            )
        if "binance" in url:
            return httpx.Response(200, json={"symbol": "BTCUSDT", "price": "67000"})
        if "coinbase" in url or "kraken" in url:
            return httpx.Response(451, text="unreachable in test")
        raise AssertionError(f"unexpected url {url}")

    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_run_processes_two_rounds_and_writes_expected_records(tmp_path):
    repo_dir = _init_repo(tmp_path)
    requested_slugs = []
    transport = _make_http_client(requested_slugs)

    rtds = FakeSimpleWSClient(
        {
            "crypto_prices": {"value": 67000.0, "feed_ts_ms": 1717000060000},
            "crypto_prices_chainlink": {"value": 66999.0, "feed_ts_ms": 1717000060000},
        }
    )
    clob_ws = FakeClobWSClient(
        {
            "111": {"book_side": _book_side(), "venue_ts_ms": 1717000060000},
            "222": {"book_side": _book_side(), "venue_ts_ms": 1717000060000},
        }
    )
    clock = FakeClock(start_ms=(ALIGNED_EPOCH_S - 10) * 1000)

    async with httpx.AsyncClient(transport=transport) as http_client:
        runner = LongjobRunner(
            http_client=http_client,
            rtds_client=rtds,
            clob_ws_client=clob_ws,
            clock=clock,
            state_path=repo_dir / "state" / "longjob.json",
            raw_base_dir=repo_dir / "data" / "raw",
            coverage_base_dir=repo_dir / "data" / "coverage",
            rejected_base_dir=repo_dir / "data" / "rejected",
            repo_dir=repo_dir,
            job_duration_sec=650,
            shutdown_margin_sec=60,
            commit_interval_sec=200,
        )
        await runner.run()

    assert runner.rounds_seen == 2
    assert runner.rounds_missed == 0
    assert runner.discovery_slug_hits == 2
    assert runner.discovery_listing_hits == 0
    assert rtds.ran is True
    assert clob_ws.ran is True
    assert len(clob_ws.subscribe_calls) == 2
    assert clob_ws.subscribe_calls[0] == ["111", "222"]

    date_str = datetime.fromtimestamp(ALIGNED_EPOCH_S, tz=timezone.utc).strftime("%Y-%m-%d")
    rounds_path = repo_dir / "data" / "raw" / "runner=longjob" / f"date={date_str}" / "rounds.jsonl"
    lines = rounds_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    round1 = json.loads(lines[0])
    assert len(round1["observations"]) == 24  # 12 offset * 2 transport
    assert round1["status"] in ("complete", "partial")
    assert round1["decision"] is None

    heartbeat_dir = repo_dir / "data" / "coverage" / "runner=longjob"
    heartbeat_files = list(heartbeat_dir.rglob("heartbeat.jsonl"))
    assert heartbeat_files
    heartbeat_lines = [json.loads(line) for line in heartbeat_files[0].read_text(encoding="utf-8").splitlines()]
    assert heartbeat_lines[0]["event"] == "job_start"
    assert heartbeat_lines[-1]["event"] == "job_end"
    assert heartbeat_lines[-1]["discovery_slug_hits"] == 2
    assert heartbeat_lines[-1]["discovery_listing_hits"] == 0
    assert "discovery_slug_hits" not in heartbeat_lines[0]  # job_start'ta yok (K-21)

    log = _git(["log", "--oneline"], cwd=repo_dir).stdout
    assert "longjob" in log  # en az bir veri commit'i atildi

    # push varsayilan kapali -- origin remote hic yok, hata da olmamali
    assert requested_slugs[0] == f"btc-updown-5m-{ALIGNED_EPOCH_S}"
    assert requested_slugs[1] == f"btc-updown-5m-{ALIGNED_EPOCH_S + 300}"


@pytest.mark.asyncio
async def test_run_resumes_from_saved_state(tmp_path):
    from collector.state import LongjobState, save_state

    repo_dir = _init_repo(tmp_path)
    state_path = repo_dir / "state" / "longjob.json"
    save_state(LongjobState(last_processed_round_epoch_s=ALIGNED_EPOCH_S), state_path)

    requested_slugs = []
    transport = _make_http_client(requested_slugs)

    rtds = FakeSimpleWSClient({})
    clob_ws = FakeClobWSClient({})
    clock = FakeClock(start_ms=(ALIGNED_EPOCH_S + 5) * 1000)

    async with httpx.AsyncClient(transport=transport) as http_client:
        runner = LongjobRunner(
            http_client=http_client,
            rtds_client=rtds,
            clob_ws_client=clob_ws,
            clock=clock,
            state_path=state_path,
            raw_base_dir=repo_dir / "data" / "raw",
            coverage_base_dir=repo_dir / "data" / "coverage",
            rejected_base_dir=repo_dir / "data" / "rejected",
            repo_dir=repo_dir,
            job_duration_sec=350,
            shutdown_margin_sec=60,
            commit_interval_sec=200,
        )
        await runner.run()

    assert requested_slugs[0] == f"btc-updown-5m-{ALIGNED_EPOCH_S + 300}"


@pytest.mark.asyncio
async def test_run_counts_missed_round_when_market_not_found(tmp_path):
    repo_dir = _init_repo(tmp_path)

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "/events" in url:
            return httpx.Response(200, json=[])
        if "/book" in url:
            raise AssertionError("book should not be called when market missing")
        # exchange_probe job baslangicinda bir kez calisir, round'dan bagimsiz
        return httpx.Response(200, json={"symbol": "BTCUSDT", "price": "67000"})

    transport = httpx.MockTransport(handler)
    rtds = FakeSimpleWSClient({})
    clob_ws = FakeClobWSClient({})
    clock = FakeClock(start_ms=(ALIGNED_EPOCH_S - 10) * 1000)

    async with httpx.AsyncClient(transport=transport) as http_client:
        runner = LongjobRunner(
            http_client=http_client,
            rtds_client=rtds,
            clob_ws_client=clob_ws,
            clock=clock,
            state_path=repo_dir / "state" / "longjob.json",
            raw_base_dir=repo_dir / "data" / "raw",
            coverage_base_dir=repo_dir / "data" / "coverage",
            rejected_base_dir=repo_dir / "data" / "rejected",
            repo_dir=repo_dir,
            job_duration_sec=350,
            shutdown_margin_sec=60,
            commit_interval_sec=200,
        )
        await runner.run()

    assert runner.rounds_seen == 0
    assert runner.rounds_missed == 1
    assert clob_ws.subscribe_calls == []
    assert not (repo_dir / "data" / "raw").exists()


@pytest.mark.asyncio
async def test_run_tracks_discovery_hits_when_second_round_falls_back_to_listing(tmp_path):
    """K-21: round1 slug ile bulunur, round2'nin slug'i bulunamaz ve
    listelemeye duser -- job_end heartbeat'i 1/1 sayar."""
    repo_dir = _init_repo(tmp_path)
    round2_epoch = ALIGNED_EPOCH_S + 300

    def _book_response():
        return httpx.Response(
            200,
            json={
                "timestamp": "1717000060000",
                "bids": [{"price": "0.48", "size": "10"}],
                "asks": [{"price": "0.52", "size": "8"}],
            },
        )

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        params = request.url.params
        if "slug" in params:
            slug = params["slug"]
            if slug == f"btc-updown-5m-{ALIGNED_EPOCH_S}":
                return httpx.Response(
                    200,
                    json=[
                        {
                            "id": "evt-1",
                            "slug": slug,
                            "conditionId": "0xabc",
                            "outcomes": json.dumps(["Up", "Down"]),
                            "clobTokenIds": json.dumps(["111", "222"]),
                        }
                    ],
                )
            return httpx.Response(200, json=[])  # round2 slug bulunamadi -> listelemeye duser
        if "active" in params:  # listeleme cagrisi
            return httpx.Response(
                200,
                json=[
                    {
                        "id": "evt-2",
                        "slug": f"btc-updown-5m-{round2_epoch}",
                        "conditionId": "0xdef",
                        "outcomes": json.dumps(["Up", "Down"]),
                        "clobTokenIds": json.dumps(["333", "444"]),
                    }
                ],
            )
        if "/book" in url:
            return _book_response()
        if "binance" in url:
            return httpx.Response(200, json={"symbol": "BTCUSDT", "price": "67000"})
        if "coinbase" in url or "kraken" in url:
            return httpx.Response(451, text="unreachable in test")
        raise AssertionError(f"unexpected url {url}")

    transport = httpx.MockTransport(handler)
    rtds = FakeSimpleWSClient(
        {
            "crypto_prices": {"value": 67000.0, "feed_ts_ms": 1717000060000},
            "crypto_prices_chainlink": {"value": 66999.0, "feed_ts_ms": 1717000060000},
        }
    )
    clob_ws = FakeClobWSClient(
        {
            "111": {"book_side": _book_side(), "venue_ts_ms": 1717000060000},
            "222": {"book_side": _book_side(), "venue_ts_ms": 1717000060000},
            "333": {"book_side": _book_side(), "venue_ts_ms": 1717000060000},
            "444": {"book_side": _book_side(), "venue_ts_ms": 1717000060000},
        }
    )
    clock = FakeClock(start_ms=(ALIGNED_EPOCH_S - 10) * 1000)

    async with httpx.AsyncClient(transport=transport) as http_client:
        runner = LongjobRunner(
            http_client=http_client,
            rtds_client=rtds,
            clob_ws_client=clob_ws,
            clock=clock,
            state_path=repo_dir / "state" / "longjob.json",
            raw_base_dir=repo_dir / "data" / "raw",
            coverage_base_dir=repo_dir / "data" / "coverage",
            rejected_base_dir=repo_dir / "data" / "rejected",
            repo_dir=repo_dir,
            job_duration_sec=650,
            shutdown_margin_sec=60,
            commit_interval_sec=200,
        )
        await runner.run()

    assert runner.rounds_seen == 2
    assert runner.discovery_slug_hits == 1
    assert runner.discovery_listing_hits == 1

    heartbeat_dir = repo_dir / "data" / "coverage" / "runner=longjob"
    heartbeat_files = list(heartbeat_dir.rglob("heartbeat.jsonl"))
    job_end = json.loads(heartbeat_files[0].read_text(encoding="utf-8").splitlines()[-1])
    assert job_end["event"] == "job_end"
    assert job_end["discovery_slug_hits"] == 1
    assert job_end["discovery_listing_hits"] == 1

    date_str = datetime.fromtimestamp(ALIGNED_EPOCH_S, tz=timezone.utc).strftime("%Y-%m-%d")
    rounds_path = repo_dir / "data" / "raw" / "runner=longjob" / f"date={date_str}" / "rounds.jsonl"
    lines = [json.loads(line) for line in rounds_path.read_text(encoding="utf-8").splitlines()]
    assert lines[0]["raw"][0]["endpoint"] == "gamma_event_slug"
    assert lines[1]["raw"][0]["endpoint"] == "gamma_event_listing"
