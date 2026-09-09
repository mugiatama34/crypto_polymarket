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
            "crypto_prices": {"value": 67000.0, "feed_ts_ms": 1717000060000, "feed_ts_source": "point"},
            "crypto_prices_chainlink": {"value": 66999.0, "feed_ts_ms": 1717000060000, "feed_ts_source": "point"},
        }
    )
    # K-25/K-29: gercek RTDSClient bu sayaclari tasir, test double elle
    # isaretliyor -- job_end'e dogru gectigini dogrulamak icin.
    rtds.dropped_not_json = 3
    rtds.dropped_unknown_symbol = 1
    rtds.dropped_unknown_shape = 2
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
            ws_leg_enabled=True,  # K-32: varsayilan kapali, bu test ws yolunu kasitli aciyor
        )
        await runner.run()

    assert runner.rounds_seen == 2
    assert runner.rounds_missed == 0
    assert runner.rounds_error == 0
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
    assert len(round1["observations"]) == 24  # ws_leg_enabled=True: 12 offset * 2 transport
    assert round1["status"] in ("complete", "partial")
    assert round1["decision"] is None
    # K-35: FakeClock hedefe tam zamaninda sicradigi icin sapma 0 -- tolerans icinde.
    assert round1["timing_valid"] is True

    heartbeat_dir = repo_dir / "data" / "coverage" / "runner=longjob"
    heartbeat_files = list(heartbeat_dir.rglob("heartbeat.jsonl"))
    assert heartbeat_files
    heartbeat_lines = [json.loads(line) for line in heartbeat_files[0].read_text(encoding="utf-8").splitlines()]
    assert heartbeat_lines[0]["event"] == "job_start"
    assert "ws_leg=on" in heartbeat_lines[0]["detail"]
    assert heartbeat_lines[-1]["event"] == "job_end"
    assert heartbeat_lines[-1]["discovery_slug_hits"] == 2
    assert heartbeat_lines[-1]["discovery_listing_hits"] == 0
    assert heartbeat_lines[-1]["rounds_error"] == 0
    assert "discovery_slug_hits" not in heartbeat_lines[0]  # job_start'ta yok (K-21)
    # K-25/K-29: rtds_client'in dusen cerceve sayaclari job_end'e geciyor.
    assert heartbeat_lines[-1]["rtds_dropped_not_json"] == 3
    assert heartbeat_lines[-1]["rtds_dropped_unknown_symbol"] == 1
    assert heartbeat_lines[-1]["rtds_dropped_unknown_shape"] == 2
    # K-32: clob_ws test double'inda bu sayaclar yok -- getattr(...,0) guvenli varsayilan.
    assert heartbeat_lines[-1]["clob_ws_dropped_not_json"] == 0
    assert heartbeat_lines[-1]["clob_ws_dropped_unknown_event_type"] == 0
    assert heartbeat_lines[-1]["clob_ws_dropped_unknown_shape"] == 0
    assert "rtds_dropped_not_json" not in heartbeat_lines[0]  # job_start'ta yok

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
async def test_run_skips_stale_backlog_and_jumps_to_current_round(tmp_path):
    """K-34: state 2 turdan cok geride kalmissa (burada ~10 tur, 3000sn)
    backlog hic islenmez -- runner dogrudan guncel tur sinirindan baslar,
    atlanan tur sayisi heartbeat error'unda ve job_end sayacinda gorunur."""
    from collector.state import LongjobState, save_state

    repo_dir = _init_repo(tmp_path)
    state_path = repo_dir / "state" / "longjob.json"
    save_state(LongjobState(last_processed_round_epoch_s=ALIGNED_EPOCH_S), state_path)

    requested_slugs = []
    transport = _make_http_client(requested_slugs)

    rtds = FakeSimpleWSClient({})
    clob_ws = FakeClobWSClient({})
    # state'teki noktadan ~1 saat (12 tur) sonrasi -- 2 tur esiginin cok uzerinde.
    now_epoch_s = ALIGNED_EPOCH_S + 3600
    clock = FakeClock(start_ms=(now_epoch_s - 10) * 1000)

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

    # backlog'daki eski round (ALIGNED_EPOCH_S+300) hic istenmedi -- dogrudan guncel tura atlandi.
    assert requested_slugs[0] == f"btc-updown-5m-{now_epoch_s}"
    assert runner.rounds_skipped_stale == 10

    heartbeat_dir = repo_dir / "data" / "coverage" / "runner=longjob"
    heartbeat_lines = [
        json.loads(line)
        for line in list(heartbeat_dir.rglob("heartbeat.jsonl"))[0].read_text(encoding="utf-8").splitlines()
    ]
    skip_errors = [h["detail"] for h in heartbeat_lines if h["event"] == "error" and "backlog atlandi" in (h["detail"] or "")]
    assert len(skip_errors) == 1
    assert heartbeat_lines[-1]["event"] == "job_end"
    assert heartbeat_lines[-1]["rounds_skipped_stale"] == 10


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
            "crypto_prices": {"value": 67000.0, "feed_ts_ms": 1717000060000, "feed_ts_source": "point"},
            "crypto_prices_chainlink": {"value": 66999.0, "feed_ts_ms": 1717000060000, "feed_ts_source": "point"},
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
    # K-25/K-29: bu test double'in dropped_* sayaclari yok -- runner.py
    # getattr(..., 0) ile guvenli varsayilana duser, KeyError/AttributeError yok.
    assert job_end["rtds_dropped_not_json"] == 0
    assert job_end["rtds_dropped_unknown_symbol"] == 0
    assert job_end["rtds_dropped_unknown_shape"] == 0

    date_str = datetime.fromtimestamp(ALIGNED_EPOCH_S, tz=timezone.utc).strftime("%Y-%m-%d")
    rounds_path = repo_dir / "data" / "raw" / "runner=longjob" / f"date={date_str}" / "rounds.jsonl"
    lines = [json.loads(line) for line in rounds_path.read_text(encoding="utf-8").splitlines()]
    assert lines[0]["raw"][0]["endpoint"] == "gamma_event_slug"
    assert lines[1]["raw"][0]["endpoint"] == "gamma_event_listing"


@pytest.mark.asyncio
async def test_ws_leg_disabled_by_default_produces_rest_only_rounds(tmp_path):
    """K-32: varsayilan `ws_leg_enabled=False` -- RTDS/CLOB WS .run() hic
    cagrilmaz, subscribe hic yapilmaz, tur basina 12 gozlem (rest-only)."""
    repo_dir = _init_repo(tmp_path)
    requested_slugs = []
    transport = _make_http_client(requested_slugs)

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
        assert runner.ws_leg_enabled is False
        await runner.run()

    assert rtds.ran is False
    assert clob_ws.ran is False
    assert clob_ws.subscribe_calls == []

    date_str = datetime.fromtimestamp(ALIGNED_EPOCH_S, tz=timezone.utc).strftime("%Y-%m-%d")
    rounds_path = repo_dir / "data" / "raw" / "runner=longjob" / f"date={date_str}" / "rounds.jsonl"
    lines = [json.loads(line) for line in rounds_path.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 1
    assert len(lines[0]["observations"]) == 12  # yalnizca rest, ws hic uretilmedi
    assert {o["transport"] for o in lines[0]["observations"]} == {"rest"}

    heartbeat_dir = repo_dir / "data" / "coverage" / "runner=longjob"
    heartbeat_lines = [
        json.loads(line)
        for line in list(heartbeat_dir.rglob("heartbeat.jsonl"))[0].read_text(encoding="utf-8").splitlines()
    ]
    assert "ws_leg=off" in heartbeat_lines[0]["detail"]
    assert heartbeat_lines[-1]["event"] == "job_end"


@pytest.mark.asyncio
async def test_unexpected_exception_in_one_round_increments_rounds_error_and_continues(tmp_path):
    """K-32: `_process_one_round`'daki BEKLENMEYEN bir istisna (kesif
    basarisizligi degil) turu atlar, `rounds_error`'i artirir, istisna
    tipini heartbeat'e yazar ve bir sonraki tura gecer -- koşumu oldurmez."""
    repo_dir = _init_repo(tmp_path)
    requested_slugs = []
    transport = _make_http_client(requested_slugs)

    rtds = FakeSimpleWSClient({})

    class RaisingOnceClobWSClient(FakeClobWSClient):
        async def subscribe(self, asset_ids):
            await super().subscribe(asset_ids)
            if len(self.subscribe_calls) == 1:
                raise ValueError("boom on first round")

    clob_ws = RaisingOnceClobWSClient(
        {
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
            ws_leg_enabled=True,
        )
        await runner.run()

    # round1'in subscribe'i patladi -> yazilmadi, atlandi; round2 normal islendi.
    assert runner.rounds_seen == 1
    assert runner.rounds_missed == 0
    assert runner.rounds_error == 1

    heartbeat_dir = repo_dir / "data" / "coverage" / "runner=longjob"
    heartbeat_lines = [
        json.loads(line)
        for line in list(heartbeat_dir.rglob("heartbeat.jsonl"))[0].read_text(encoding="utf-8").splitlines()
    ]
    error_details = [h["detail"] for h in heartbeat_lines if h["event"] == "error"]
    assert any("exc_type=ValueError" in d and "round isleme hatasi" in d for d in error_details)
    assert heartbeat_lines[-1]["event"] == "job_end"
    assert heartbeat_lines[-1]["rounds_error"] == 1

    date_str = datetime.fromtimestamp(ALIGNED_EPOCH_S, tz=timezone.utc).strftime("%Y-%m-%d")
    rounds_path = repo_dir / "data" / "raw" / "runner=longjob" / f"date={date_str}" / "rounds.jsonl"
    lines = [json.loads(line) for line in rounds_path.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 1  # yalnizca round2 yazildi, round1 hic yazilmadi


@pytest.mark.asyncio
async def test_job_end_written_even_when_loop_raises_past_round_guard(tmp_path):
    """K-32: point 3 -- round-seviyesi try/except'in KAPSAMADIGI bir
    istisna (burada save_state hatasi) donguden kacsa bile, finally
    blogu son commit'i ve job_end'i yazmayi garanti eder (K-06)."""
    repo_dir = _init_repo(tmp_path)
    requested_slugs = []
    transport = _make_http_client(requested_slugs)

    rtds = FakeSimpleWSClient({})
    clob_ws = FakeClobWSClient(
        {
            "111": {"book_side": _book_side(), "venue_ts_ms": 1717000060000},
            "222": {"book_side": _book_side(), "venue_ts_ms": 1717000060000},
        }
    )
    clock = FakeClock(start_ms=(ALIGNED_EPOCH_S - 10) * 1000)

    # state_path'in PARENT'i onceden bir DOSYA olarak var -- save_state
    # icindeki path.parent.mkdir(parents=True, exist_ok=True) bu yuzden
    # FileExistsError firlatir (round guard'in disinda, while govdesinde).
    blocker = repo_dir / "state_blocker"
    blocker.write_text("not a directory", encoding="utf-8")
    state_path = blocker / "longjob.json"

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
            job_duration_sec=650,
            shutdown_margin_sec=60,
            commit_interval_sec=200,
            ws_leg_enabled=True,
        )
        with pytest.raises((FileExistsError, NotADirectoryError)):
            await runner.run()

    assert runner.rounds_seen == 1  # ilk round basariyla islendi, save_state'te patladi

    heartbeat_dir = repo_dir / "data" / "coverage" / "runner=longjob"
    heartbeat_lines = [
        json.loads(line)
        for line in list(heartbeat_dir.rglob("heartbeat.jsonl"))[0].read_text(encoding="utf-8").splitlines()
    ]
    # istisna round guard'i atlayip donguden kacti, ama finally hala calisti.
    assert heartbeat_lines[-1]["event"] == "job_end"
    assert heartbeat_lines[-1]["rounds_seen"] == 1

    log = _git(["log", "--oneline"], cwd=repo_dir).stdout
    assert "longjob" in log  # finally icindeki force commit de calisti
