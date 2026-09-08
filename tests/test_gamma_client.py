import json

import httpx
import pytest

from collector.gamma_client import (
    MarketParseError,
    discover_round_market,
    fetch_round_market,
    fetch_round_market_via_listing,
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
        # K-22: startDate serinin eski olusturulma tarihi -- turun
        # baslangici DEGIL, artik open_ts adaylarinda yok. Bilerek
        # startTime'dan FARKLI/eski birakildi: yanlislikla tekrar
        # kullanilirsa asagidaki happy-path testi patlar.
        "startDate": "2000-01-01T00:00:00Z",
        "startTime": "2024-05-29T13:30:00Z",
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
    # startTime'dan (K-22) -- startDate'in yanlis/eski degerinden DEGIL.
    assert market.open_ts_ms == 1716989400000
    assert market.close_ts_ms == 1716989700000
    assert market.raw["id"] == "evt-1"
    assert market.discovery_method == "slug"


def test_parse_event_response_empty_list_returns_none():
    assert parse_event_response([], START_EPOCH_S) is None


def test_parse_event_response_falls_back_to_slug_epoch_when_no_dates():
    event = _sample_event()
    del event["startDate"]
    del event["startTime"]
    del event["endDate"]
    market = parse_event_response([event], START_EPOCH_S)
    assert market.open_ts_ms == START_EPOCH_S * 1000
    assert market.close_ts_ms == (START_EPOCH_S + 300) * 1000


def test_parse_event_response_ignores_start_date_even_when_present():
    """K-22: startDate her zaman gorunse bile (silinmese bile) open_ts
    icin hic kullanilmaz -- yalnizca startTime/eventStartTime denenir."""
    event = _sample_event()
    del event["startTime"]
    market = parse_event_response([event], START_EPOCH_S)
    # startTime yok, eventStartTime/gameStartTime de yok -> slug epoch'una
    # duser; startDate'teki 2000-01-01 DEGERI HIC KULLANILMAZ.
    assert market.open_ts_ms == START_EPOCH_S * 1000


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


@pytest.mark.asyncio
async def test_fetch_round_market_via_listing_finds_match_by_close_ts():
    """Slug deseni yanlis bile olsa (event'in gercek slug'i farkli bir
    epoch tasiyor), close_ts eslesmesiyle bulunabilir."""

    other_event = _sample_event(
        slug="btc-updown-5m-1600000000",
        conditionId="0xother",
        startDate="2000-01-01T00:00:00Z",
        endDate="2000-01-01T00:05:00Z",
    )
    matching_event = _sample_event(slug="btc-updown-5m-1717000200", conditionId="0xmatch")
    del matching_event["startDate"]
    del matching_event["endDate"]  # tarih alani yok -> slug epoch'undan (+300s) dusulur, hedefle esler

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        assert params["active"] == "true"
        assert params["closed"] == "false"
        return httpx.Response(200, json=[other_event, matching_event])

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        market = await fetch_round_market_via_listing(client, START_EPOCH_S)

    assert market is not None
    assert market.condition_id == "0xmatch"
    assert market.discovery_method == "listing"
    assert market.round_id == "btc-updown-5m-1717000200"  # bizim hedef epoch'umuz


@pytest.mark.asyncio
async def test_fetch_round_market_via_listing_paginates_until_match_or_short_page():
    page_one = [_sample_event(slug=f"btc-updown-5m-{1717000200 - 300 * i}") for i in range(1, 101)]
    page_two_event = _sample_event(slug="btc-updown-5m-1717000200", conditionId="0xpage2")
    del page_two_event["startDate"]
    del page_two_event["endDate"]  # slug epoch'undan dusulsun, hedefle essin
    page_two = [page_two_event]
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["offset"])
        calls.append(offset)
        if offset == 0:
            return httpx.Response(200, json=page_one)
        return httpx.Response(200, json=page_two)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        market = await fetch_round_market_via_listing(client, START_EPOCH_S)

    assert calls == [0, 100]
    assert market is not None
    assert market.condition_id == "0xpage2"


@pytest.mark.asyncio
async def test_fetch_round_market_via_listing_returns_none_when_no_match():
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=[]))
    async with httpx.AsyncClient(transport=transport) as client:
        market = await fetch_round_market_via_listing(client, START_EPOCH_S)
    assert market is None


@pytest.mark.asyncio
async def test_fetch_round_market_via_listing_ignores_non_matching_slug_prefix():
    unrelated = _sample_event(slug="eth-updown-5m-1717000200", conditionId="0xunrelated")
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=[unrelated]))
    async with httpx.AsyncClient(transport=transport) as client:
        market = await fetch_round_market_via_listing(client, START_EPOCH_S)
    assert market is None


@pytest.mark.asyncio
async def test_discover_round_market_uses_slug_first():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(200, json=[_sample_event()])

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        market = await discover_round_market(client, START_EPOCH_S)

    assert market.discovery_method == "slug"
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_discover_round_market_falls_back_to_listing_when_slug_empty():
    matching_event = _sample_event(slug="btc-updown-5m-1717000200", conditionId="0xlisted")
    del matching_event["startDate"]
    del matching_event["endDate"]

    def handler(request: httpx.Request) -> httpx.Response:
        if "slug" in request.url.params:
            return httpx.Response(200, json=[])
        return httpx.Response(200, json=[matching_event])

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        market = await discover_round_market(client, START_EPOCH_S)

    assert market is not None
    assert market.condition_id == "0xlisted"
    assert market.discovery_method == "listing"


@pytest.mark.asyncio
async def test_discover_round_market_falls_back_to_listing_when_slug_path_errors():
    matching_event = _sample_event(slug="btc-updown-5m-1717000200", conditionId="0xrecovered")
    del matching_event["startDate"]
    del matching_event["endDate"]

    def handler(request: httpx.Request) -> httpx.Response:
        if "slug" in request.url.params:
            return httpx.Response(500, text="boom")
        return httpx.Response(200, json=[matching_event])

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        market = await discover_round_market(client, START_EPOCH_S)

    assert market is not None
    assert market.condition_id == "0xrecovered"
    assert market.discovery_method == "listing"


@pytest.mark.asyncio
async def test_discover_round_market_returns_none_when_both_paths_fail():
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=[]))
    async with httpx.AsyncClient(transport=transport) as client:
        market = await discover_round_market(client, START_EPOCH_S)
    assert market is None
