import json

import httpx
import pytest

from collector.gamma_client import (
    MarketParseError,
    fetch_round_market,
    parse_event_response,
)

START_EPOCH_S = 1717000200  # 300s-hizali


def _sample_event(**overrides):
    event = {
        "id": "evt-1",
        "slug": "btc-updown-5m-1717000200",
        "conditionId": "0xabc123",
        "outcomes": json.dumps(["Up", "Down"]),
        "clobTokenIds": json.dumps(["111222", "333444"]),
        "startDate": "2024-05-29T13:30:00Z",
        "endDate": "2024-05-29T13:35:00Z",
    }
    event.update(overrides)
    return event


def test_parse_event_response_happy_path():
    market = parse_event_response([_sample_event()], START_EPOCH_S)
    assert market is not None
    assert market.round_id == "btc-updown-5m-1717000200"
    assert market.condition_id == "0xabc123"
    assert market.token_ids == {"up": "111222", "down": "333444"}
    assert market.open_ts_ms == 1716989400000
    assert market.close_ts_ms == 1716989700000
    assert market.raw["id"] == "evt-1"


def test_parse_event_response_empty_list_returns_none():
    assert parse_event_response([], START_EPOCH_S) is None


def test_parse_event_response_falls_back_to_slug_epoch_when_no_dates():
    event = _sample_event()
    del event["startDate"]
    del event["endDate"]
    market = parse_event_response([event], START_EPOCH_S)
    assert market.open_ts_ms == START_EPOCH_S * 1000
    assert market.close_ts_ms == (START_EPOCH_S + 300) * 1000


def test_parse_event_response_nested_markets_array():
    event = {
        "id": "evt-2",
        "markets": [
            {
                "conditionId": "0xdef456",
                "outcomes": json.dumps(["Up", "Down"]),
                "clobTokenIds": json.dumps(["555", "666"]),
            }
        ],
    }
    market = parse_event_response([event], START_EPOCH_S)
    assert market.condition_id == "0xdef456"
    assert market.token_ids == {"up": "555", "down": "666"}


def test_parse_event_response_missing_condition_id_raises():
    event = _sample_event()
    del event["conditionId"]
    with pytest.raises(MarketParseError):
        parse_event_response([event], START_EPOCH_S)


def test_parse_event_response_outcome_mismatch_raises():
    event = _sample_event(outcomes=json.dumps(["Yes", "No"]))
    with pytest.raises(MarketParseError):
        parse_event_response([event], START_EPOCH_S)


@pytest.mark.asyncio
async def test_fetch_round_market_uses_slug_query_param():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(200, json=[_sample_event()])

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        market = await fetch_round_market(client, START_EPOCH_S)

    assert "slug=btc-updown-5m-1717000200" in captured["url"]
    assert market.condition_id == "0xabc123"


@pytest.mark.asyncio
async def test_fetch_round_market_returns_none_when_not_found():
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=[]))
    async with httpx.AsyncClient(transport=transport) as client:
        market = await fetch_round_market(client, START_EPOCH_S)
    assert market is None
