import httpx
import pytest

from collector.exchange_probe import fetch_price, probe_exchanges


@pytest.mark.asyncio
async def test_probe_exchanges_uses_coinbase_when_available():
    def handler(request: httpx.Request) -> httpx.Response:
        assert "coinbase" in str(request.url)
        return httpx.Response(200, json={"price": "67000.50", "bid": "67000", "ask": "67001"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await probe_exchanges(client)

    assert result.exchange == "coinbase"
    assert result.value == 67000.50
    assert result.attempts[0].ok is True


@pytest.mark.asyncio
async def test_probe_exchanges_falls_back_to_kraken_on_coinbase_failure():
    def handler(request: httpx.Request) -> httpx.Response:
        if "coinbase" in str(request.url):
            return httpx.Response(451, text="restricted location")
        if "kraken" in str(request.url):
            return httpx.Response(
                200,
                json={"error": [], "result": {"XXBTZUSD": {"c": ["67010.25", "0.5"]}}},
            )
        raise AssertionError("binance should not be reached")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await probe_exchanges(client)

    assert result.exchange == "kraken"
    assert result.value == 67010.25
    assert result.attempts[0].exchange == "coinbase"
    assert result.attempts[0].ok is False
    assert result.attempts[0].status_code == 451
    assert result.attempts[1].exchange == "kraken"
    assert result.attempts[1].ok is True


@pytest.mark.asyncio
async def test_probe_exchanges_falls_back_to_binance_when_coinbase_and_kraken_fail():
    """K-19: Binance her seferinde 451 dondugu icin sirada son -- yalnizca
    Coinbase ve Kraken ikisi de basarisiz olursa denenir."""

    def handler(request: httpx.Request) -> httpx.Response:
        if "binance" in str(request.url):
            return httpx.Response(200, json={"symbol": "BTCUSDT", "price": "67020.1"})
        return httpx.Response(451, text="restricted location")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await probe_exchanges(client)

    assert result.exchange == "binance"
    assert result.value == 67020.1


@pytest.mark.asyncio
async def test_probe_exchanges_all_blocked_returns_none():
    transport = httpx.MockTransport(lambda request: httpx.Response(451, text="blocked"))
    async with httpx.AsyncClient(transport=transport) as client:
        result = await probe_exchanges(client)

    assert result.exchange is None
    assert result.value is None
    assert len(result.attempts) == 3
    assert all(not a.ok for a in result.attempts)


@pytest.mark.asyncio
async def test_probe_exchanges_connection_error_falls_through():
    def handler(request: httpx.Request) -> httpx.Response:
        if "coinbase" in str(request.url):
            raise httpx.ConnectError("connection refused")
        return httpx.Response(
            200,
            json={"error": [], "result": {"XXBTZUSD": {"c": ["1.0", "0.5"]}}},
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await probe_exchanges(client)

    assert result.exchange == "kraken"
    assert result.attempts[0].error == "connection refused"


@pytest.mark.asyncio
async def test_fetch_price_uses_only_given_exchange_no_fallback():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(200, json={"symbol": "BTCUSDT", "price": "68000"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await fetch_price(client, "binance")

    assert len(calls) == 1
    assert "binance" in calls[0]
    assert result.value == 68000.0


@pytest.mark.asyncio
async def test_fetch_price_failure_returns_none_value_not_exception():
    transport = httpx.MockTransport(lambda request: httpx.Response(451, text="blocked"))
    async with httpx.AsyncClient(transport=transport) as client:
        result = await fetch_price(client, "binance")

    assert result.value is None
    assert result.attempts[0].ok is False


@pytest.mark.asyncio
async def test_probe_exchanges_coinbase_extracts_feed_ts_ms():
    """K-36: Coinbase'in "time" alani (ISO8601, prob'da gorulen bicim)
    feed_ts_ms'e (epoch ms) cevriliyor."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "price": "67000.50",
                "bid": "67000",
                "ask": "67001",
                "time": "2026-09-08T18:29:53.773177744Z",
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await probe_exchanges(client)

    assert result.exchange == "coinbase"
    assert result.feed_ts_ms == 1788892193773


@pytest.mark.asyncio
async def test_probe_exchanges_coinbase_missing_time_field_leaves_feed_ts_none():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"price": "67000.50", "bid": "67000", "ask": "67001"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await probe_exchanges(client)

    assert result.exchange == "coinbase"
    assert result.feed_ts_ms is None


@pytest.mark.asyncio
async def test_probe_exchanges_kraken_has_no_feed_ts():
    """K-36: Kraken'in Ticker ucunda quote'un kendi zaman damgasi yok --
    ("t" alani trade sayisi, zaman degil) feed_ts_ms hep None kalir."""

    def handler(request: httpx.Request) -> httpx.Response:
        if "coinbase" in str(request.url):
            return httpx.Response(451, text="restricted location")
        if "kraken" in str(request.url):
            return httpx.Response(
                200,
                json={"error": [], "result": {"XXBTZUSD": {"c": ["67010.25", "0.5"]}}},
            )
        raise AssertionError("binance should not be reached")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await probe_exchanges(client)

    assert result.exchange == "kraken"
    assert result.feed_ts_ms is None
