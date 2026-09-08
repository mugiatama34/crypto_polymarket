import httpx
import pytest

from collector.exchange_probe import fetch_price, probe_exchanges


@pytest.mark.asyncio
async def test_probe_exchanges_uses_binance_when_available():
    def handler(request: httpx.Request) -> httpx.Response:
        assert "binance" in str(request.url)
        return httpx.Response(200, json={"symbol": "BTCUSDT", "price": "67000.50"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await probe_exchanges(client)

    assert result.exchange == "binance"
    assert result.value == 67000.50
    assert result.attempts[0].ok is True


@pytest.mark.asyncio
async def test_probe_exchanges_falls_back_to_coinbase_on_binance_451():
    def handler(request: httpx.Request) -> httpx.Response:
        if "binance" in str(request.url):
            return httpx.Response(451, text="restricted location")
        if "coinbase" in str(request.url):
            return httpx.Response(200, json={"price": "67010.25", "bid": "67009", "ask": "67011"})
        raise AssertionError("kraken should not be reached")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await probe_exchanges(client)

    assert result.exchange == "coinbase"
    assert result.value == 67010.25
    assert result.attempts[0].exchange == "binance"
    assert result.attempts[0].ok is False
    assert result.attempts[0].status_code == 451
    assert result.attempts[1].exchange == "coinbase"
    assert result.attempts[1].ok is True


@pytest.mark.asyncio
async def test_probe_exchanges_falls_back_to_kraken_when_binance_and_coinbase_fail():
    def handler(request: httpx.Request) -> httpx.Response:
        if "kraken" in str(request.url):
            return httpx.Response(
                200,
                json={"error": [], "result": {"XXBTZUSD": {"c": ["67020.1", "0.5"]}}},
            )
        return httpx.Response(451, text="restricted location")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await probe_exchanges(client)

    assert result.exchange == "kraken"
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
        if "binance" in str(request.url):
            raise httpx.ConnectError("connection refused")
        return httpx.Response(200, json={"price": "1.0", "bid": "1", "ask": "1"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await probe_exchanges(client)

    assert result.exchange == "coinbase"
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
