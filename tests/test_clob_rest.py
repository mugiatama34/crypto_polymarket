import httpx
import pytest

from collector.clob_rest import fetch_book


@pytest.mark.asyncio
async def test_fetch_book_parses_bids_asks_and_timestamp():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(
            200,
            json={
                "market": "0xcond",
                "asset_id": "111",
                "timestamp": "1717000060000",
                "bids": [{"price": "0.48", "size": "10"}],
                "asks": [{"price": "0.52", "size": "8"}],
                "hash": "0xhash",
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await fetch_book(client, "111")

    assert "token_id=111" in captured["url"]
    assert result.venue_ts_ms == 1717000060000
    assert result.book_side["best_bid"] == 0.48
    assert result.book_side["best_ask"] == 0.52
    assert result.raw["hash"] == "0xhash"


@pytest.mark.asyncio
async def test_fetch_book_missing_timestamp_is_none():
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, json={"bids": [], "asks": []})
    )
    async with httpx.AsyncClient(transport=transport) as client:
        result = await fetch_book(client, "222")
    assert result.venue_ts_ms is None
