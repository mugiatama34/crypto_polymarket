import httpx
import pytest

from collector.sampler import build_rest_observation, build_ws_observation
from validator.core import validate

TOKEN_IDS = {"up": "111", "down": "222"}
CLOSE_TS_MS = 1717000300000


class FakeCache:
    def __init__(self, data):
        self._data = data

    def snapshot(self, key):
        return self._data.get(key)


def _clock(values):
    it = iter(values)
    return lambda: next(it)


def _full_book_side():
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


def _wrap_round(observation):
    return {
        "schema_version": 2,
        "runner_id": "longjob",
        "job_id": "job-test",
        "data_lane": "forward_paper",
        "round_id": "btc-updown-5m-1717000000",
        "condition_id": "0xabc",
        "token_ids": TOKEN_IDS,
        "open_ts": 1717000000000,
        "close_ts": CLOSE_TS_MS,
        "observations": [observation],
        "decision": None,
        "status": "partial",
        "timing_valid": True,
        "raw": [],
    }


@pytest.mark.asyncio
async def test_build_ws_observation_all_present_is_ok_and_schema_valid():
    clob_ws = FakeCache(
        {
            "111": {"book_side": _full_book_side(), "venue_ts_ms": 1717000060000},
            "222": {"book_side": _full_book_side(), "venue_ts_ms": 1717000060000},
        }
    )
    rtds = FakeCache(
        {
            "crypto_prices": {"value": 67000.0, "feed_ts_ms": 1717000060000, "feed_ts_source": "point"},
            "crypto_prices_chainlink": {"value": 66998.0, "feed_ts_ms": 1717000059800, "feed_ts_source": "point"},
        }
    )
    now = _clock([1717000060010, 1717000060010, 1717000060015])

    observation, raw_entries = await build_ws_observation(
        offset_sec=240,
        close_ts_ms=CLOSE_TS_MS,
        token_ids=TOKEN_IDS,
        rtds_client=rtds,
        clob_ws_client=clob_ws,
        now_ms_fn=now,
    )

    assert observation["status"] == "ok"
    assert observation["transport"] == "ws"
    assert observation["error"] is None
    # K-20: ws bacaginda latency_ms anlamli degil (cache kopyalama), her
    # zaman null; tazelik staleness_ms'te.
    assert observation["latency_ms"] is None
    assert observation["staleness_ms"] == observation["response_ts"] - 1717000060000
    assert observation["btc_reference"]["source"] == "rtds_binance"
    assert observation["btc_reference"]["venue"] == "polymarket_rtds"
    assert observation["btc_oracle"]["source"] == "rtds_chainlink"
    assert observation["btc_oracle"]["venue"] == "chainlink"
    assert raw_entries == []

    ok, errors = validate(_wrap_round(observation), "round")
    assert ok, errors


@pytest.mark.asyncio
async def test_build_ws_observation_includes_rtds_raw_envelope_in_raw_entries():
    """RTDS'in ham zarfi (varsa) raw[]'a girer -- onceden hep [] donuyordu,
    yani publish_ts_ms gibi extra alanlar kalici olarak kayboluyordu
    (bkz. docs/decisions.md K-22 sonrasi tartisma, collector/sampler.py)."""
    clob_ws = FakeCache(
        {
            "111": {"book_side": _full_book_side(), "venue_ts_ms": 1717000060000},
            "222": {"book_side": _full_book_side(), "venue_ts_ms": 1717000060000},
        }
    )
    binance_envelope = {"topic": "crypto_prices", "type": "update", "timestamp": 1717000060005, "payload": {}}
    chainlink_envelope = {
        "topic": "crypto_prices_chainlink",
        "type": "update",
        "timestamp": 1717000059805,
        "payload": {},
    }
    rtds = FakeCache(
        {
            "crypto_prices": {
                "value": 67000.0,
                "feed_ts_ms": 1717000060000,
                "feed_ts_source": "point",
                "publish_ts_ms": 1717000060005,
                "raw_envelope": binance_envelope,
            },
            "crypto_prices_chainlink": {
                "value": 66998.0,
                "feed_ts_ms": 1717000059800,
                "feed_ts_source": "point",
                "publish_ts_ms": 1717000059805,
                "raw_envelope": chainlink_envelope,
            },
        }
    )
    now = _clock([1717000060010, 1717000060010, 1717000060015])

    _, raw_entries = await build_ws_observation(
        offset_sec=240,
        close_ts_ms=CLOSE_TS_MS,
        token_ids=TOKEN_IDS,
        rtds_client=rtds,
        clob_ws_client=clob_ws,
        now_ms_fn=now,
    )

    assert raw_entries == [
        {"endpoint": "rtds_binance", "payload": binance_envelope},
        {"endpoint": "rtds_chainlink", "payload": chainlink_envelope},
    ]


@pytest.mark.asyncio
async def test_build_ws_observation_partial_when_oracle_missing():
    clob_ws = FakeCache(
        {
            "111": {"book_side": _full_book_side(), "venue_ts_ms": 1717000060000},
            "222": {"book_side": _full_book_side(), "venue_ts_ms": 1717000060000},
        }
    )
    rtds = FakeCache({"crypto_prices": {"value": 67000.0, "feed_ts_ms": 1717000060000, "feed_ts_source": "point"}})
    now = _clock([1, 1, 2])

    observation, _ = await build_ws_observation(
        offset_sec=60,
        close_ts_ms=CLOSE_TS_MS,
        token_ids=TOKEN_IDS,
        rtds_client=rtds,
        clob_ws_client=clob_ws,
        now_ms_fn=now,
    )

    assert observation["status"] == "partial"
    assert observation["btc_oracle"] == {
        "value": None,
        "source": "none",
        "venue": "none",
        "feed_ts": None,
        "feed_ts_source": "none",
    }
    assert "btc_oracle" in observation["error"]


@pytest.mark.asyncio
async def test_build_ws_observation_missed_when_everything_empty_uses_runner_ts_as_venue_ts():
    clob_ws = FakeCache({})
    rtds = FakeCache({})
    now = _clock([5000, 5000, 5001])

    observation, _ = await build_ws_observation(
        offset_sec=10,
        close_ts_ms=CLOSE_TS_MS,
        token_ids=TOKEN_IDS,
        rtds_client=rtds,
        clob_ws_client=clob_ws,
        now_ms_fn=now,
    )

    assert observation["status"] == "missed"
    assert observation["venue_ts"] == observation["runner_ts"]
    assert observation["latency_ms"] is None
    assert observation["staleness_ms"] is None
    assert observation["book"]["up"]["bids_top5"] == []

    ok, errors = validate(_wrap_round(observation), "round")
    assert ok, errors


@pytest.mark.asyncio
async def test_build_rest_observation_ok_with_exchange():
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "token_id=111" in url:
            return httpx.Response(
                200,
                json={"timestamp": "1717000061000", "bids": [{"price": "0.48", "size": "10"}], "asks": [{"price": "0.52", "size": "8"}]},
            )
        if "token_id=222" in url:
            return httpx.Response(
                200,
                json={"timestamp": "1717000061000", "bids": [{"price": "0.46", "size": "5"}], "asks": [{"price": "0.5", "size": "6"}]},
            )
        if "binance" in url:
            return httpx.Response(200, json={"symbol": "BTCUSDT", "price": "67001.5"})
        raise AssertionError(f"unexpected url {url}")

    transport = httpx.MockTransport(handler)
    now = _clock([1717000061010, 1717000061010, 1717000061040])

    async with httpx.AsyncClient(transport=transport) as client:
        observation, raw_entries = await build_rest_observation(
            offset_sec=180,
            close_ts_ms=CLOSE_TS_MS,
            token_ids=TOKEN_IDS,
            http_client=client,
            exchange="binance",
            now_ms_fn=now,
        )

    assert observation["status"] == "ok"
    assert observation["transport"] == "rest"
    # K-20: rest bacaginda latency_ms gercek RTT, dolu; staleness_ms
    # feed_ts REST'te donmedigi icin null.
    assert observation["latency_ms"] == observation["response_ts"] - observation["runner_ts"]
    assert observation["staleness_ms"] is None
    assert observation["btc_reference"] == {
        "value": 67001.5,
        "source": "rest_poll",
        "venue": "binance",
        "feed_ts": None,
        "feed_ts_source": "none",
    }
    assert observation["btc_oracle"] == {
        "value": None,
        "source": "none",
        "venue": "none",
        "feed_ts": None,
        "feed_ts_source": "none",
    }
    assert len(raw_entries) == 3

    ok, errors = validate(_wrap_round(observation), "round")
    assert ok, errors


@pytest.mark.asyncio
async def test_build_rest_observation_partial_when_book_down_fails():
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "token_id=111" in url:
            return httpx.Response(
                200, json={"timestamp": "1717000061000", "bids": [{"price": "0.48", "size": "10"}], "asks": []}
            )
        if "token_id=222" in url:
            return httpx.Response(500, text="boom")
        if "binance" in url:
            return httpx.Response(200, json={"symbol": "BTCUSDT", "price": "67001.5"})
        raise AssertionError(f"unexpected url {url}")

    transport = httpx.MockTransport(handler)
    now = _clock([1, 1, 2])

    async with httpx.AsyncClient(transport=transport) as client:
        observation, _ = await build_rest_observation(
            offset_sec=105,
            close_ts_ms=CLOSE_TS_MS,
            token_ids=TOKEN_IDS,
            http_client=client,
            exchange="binance",
            now_ms_fn=now,
        )

    assert observation["status"] == "partial"
    assert "book.down" in observation["error"]


@pytest.mark.asyncio
async def test_build_rest_observation_no_exchange_available():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"timestamp": "1717000061000", "bids": [{"price": "0.48", "size": "10"}], "asks": [{"price": "0.52", "size": "8"}]}
        )

    transport = httpx.MockTransport(handler)
    now = _clock([1, 1, 2])

    async with httpx.AsyncClient(transport=transport) as client:
        observation, _ = await build_rest_observation(
            offset_sec=30,
            close_ts_ms=CLOSE_TS_MS,
            token_ids=TOKEN_IDS,
            http_client=client,
            exchange=None,
            now_ms_fn=now,
        )

    assert observation["status"] == "ok"  # yalnizca book attempt edildi, ikisi de basarili
    assert observation["btc_reference"] == {
        "value": None,
        "source": "none",
        "venue": "none",
        "feed_ts": None,
        "feed_ts_source": "none",
    }


@pytest.mark.asyncio
async def test_build_rest_observation_coinbase_populates_feed_ts_and_staleness():
    """K-36: Coinbase yanitindaki "time" alani artik feed_ts'e taniniyor --
    staleness_ms de (response_ts - feed_ts) bu sayede hesaplanabiliyor,
    onceden REST bacaginda her zaman None donuyordu."""
    import datetime as dt_module

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "token_id" in url:
            return httpx.Response(
                200,
                json={"timestamp": "1717000061000", "bids": [{"price": "0.48", "size": "10"}], "asks": [{"price": "0.52", "size": "8"}]},
            )
        if "coinbase" in url:
            return httpx.Response(
                200,
                json={"price": "67005.0", "bid": "67004", "ask": "67006", "time": "2024-05-30T00:01:00.500000Z"},
            )
        raise AssertionError(f"unexpected url {url}")

    transport = httpx.MockTransport(handler)
    now = _clock([1, 1, 2])

    async with httpx.AsyncClient(transport=transport) as client:
        observation, _ = await build_rest_observation(
            offset_sec=30,
            close_ts_ms=CLOSE_TS_MS,
            token_ids=TOKEN_IDS,
            http_client=client,
            exchange="coinbase",
            now_ms_fn=now,
        )

    expected_feed_ts = int(
        dt_module.datetime(2024, 5, 30, 0, 1, 0, 500000, tzinfo=dt_module.timezone.utc).timestamp() * 1000
    )
    assert observation["btc_reference"]["feed_ts"] == expected_feed_ts
    assert observation["btc_reference"]["feed_ts_source"] == "venue_rest"
    assert observation["staleness_ms"] == observation["response_ts"] - expected_feed_ts

    ok, errors = validate(_wrap_round(observation), "round")
    assert ok, errors


@pytest.mark.asyncio
async def test_build_rest_observation_exchange_fallback_changes_venue():
    """K-19: probe binance yerine coinbase'e dusmusse, venue bunu tasir."""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "token_id" in url:
            return httpx.Response(
                200,
                json={"timestamp": "1717000061000", "bids": [{"price": "0.48", "size": "10"}], "asks": [{"price": "0.52", "size": "8"}]},
            )
        if "coinbase" in url:
            return httpx.Response(200, json={"price": "67005.0", "bid": "67004", "ask": "67006"})
        raise AssertionError(f"unexpected url {url}")

    transport = httpx.MockTransport(handler)
    now = _clock([1, 1, 2])

    async with httpx.AsyncClient(transport=transport) as client:
        observation, _ = await build_rest_observation(
            offset_sec=30,
            close_ts_ms=CLOSE_TS_MS,
            token_ids=TOKEN_IDS,
            http_client=client,
            exchange="coinbase",
            now_ms_fn=now,
        )

    assert observation["btc_reference"]["venue"] == "coinbase"
    assert observation["btc_reference"]["source"] == "rest_poll"


@pytest.mark.asyncio
async def test_build_rest_observation_all_fail_is_missed():
    transport = httpx.MockTransport(lambda request: httpx.Response(500, text="down"))
    now = _clock([1, 1, 2])

    async with httpx.AsyncClient(transport=transport) as client:
        observation, _ = await build_rest_observation(
            offset_sec=30,
            close_ts_ms=CLOSE_TS_MS,
            token_ids=TOKEN_IDS,
            http_client=client,
            exchange="binance",
            now_ms_fn=now,
        )

    assert observation["status"] == "error"
    assert observation["venue_ts"] == observation["runner_ts"]
    assert observation["staleness_ms"] is None

    ok, errors = validate(_wrap_round(observation), "round")
    assert ok, errors
