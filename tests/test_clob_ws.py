import asyncio
import json

import pytest

from collector.clob_ws import ClobMarketWSClient


class FakeWebSocket:
    def __init__(self, messages):
        self._messages = messages
        self.sent = []
        self.closed_by_test = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def __aiter__(self):
        return self._iter()

    async def _iter(self):
        for message in self._messages:
            yield message
        while not self.closed_by_test:
            await asyncio.sleep(0)
        raise ConnectionResetError("fake connection closed")

    async def send(self, message):
        self.sent.append(message)


async def _instant_sleep(_seconds):
    await asyncio.sleep(0)


BOOK_EVENT = json.dumps(
    {
        "event_type": "book",
        "asset_id": "111",
        "market": "0xcond",
        "bids": [{"price": "0.48", "size": "10"}],
        "asks": [{"price": "0.52", "size": "8"}],
        "timestamp": "1717000060000",
    }
)

PRICE_CHANGE_EVENT = json.dumps(
    {
        "event_type": "price_change",
        "market": "0xcond",
        "timestamp": "1717000061000",
        "price_changes": [
            {"asset_id": "111", "price": "0.49", "size": "20", "side": "BUY"},
            {"asset_id": "111", "price": "0.48", "size": "0", "side": "BUY"},
        ],
    }
)


@pytest.mark.asyncio
async def test_subscribe_sends_assets_ids_message():
    ws = FakeWebSocket([])
    client = ClobMarketWSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    async def driver():
        while not client._ws_client._ws:
            await asyncio.sleep(0)
        await client.subscribe(["111", "222"])
        ws.closed_by_test = True
        client.stop()

    await asyncio.wait_for(asyncio.gather(client.run(), driver()), timeout=5)

    sent = json.loads(ws.sent[0])
    assert sent == {"assets_ids": ["111", "222"], "type": "market", "custom_feature_enabled": True}


@pytest.mark.asyncio
async def test_book_snapshot_populates_cache():
    ws = FakeWebSocket([BOOK_EVENT])
    client = ClobMarketWSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    async def stop_soon():
        while client.snapshot("111") is None:
            await asyncio.sleep(0)
        ws.closed_by_test = True
        client.stop()

    await asyncio.wait_for(asyncio.gather(client.run(), stop_soon()), timeout=5)

    snap = client.snapshot("111")
    assert snap["venue_ts_ms"] == 1717000060000
    assert snap["book_side"]["best_bid"] == 0.48
    assert snap["book_side"]["best_ask"] == 0.52


@pytest.mark.asyncio
async def test_price_change_applied_on_top_of_book_snapshot():
    ws = FakeWebSocket([BOOK_EVENT, PRICE_CHANGE_EVENT])
    client = ClobMarketWSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    async def stop_soon():
        while client.snapshot("111") is None or client.snapshot("111")["venue_ts_ms"] != 1717000061000:
            await asyncio.sleep(0)
        ws.closed_by_test = True
        client.stop()

    await asyncio.wait_for(asyncio.gather(client.run(), stop_soon()), timeout=5)

    snap = client.snapshot("111")
    # 0.48 seviyesi size=0 ile kaldirildi, 0.49 eklendi -> yeni best_bid 0.49
    assert snap["book_side"]["best_bid"] == 0.49
    assert snap["book_side"]["bid_size"] == 20.0
    assert [0.48, 10.0] not in snap["book_side"]["bids_top5"]


NOT_JSON_MESSAGE = "not valid json {"

UNKNOWN_EVENT_TYPE_MESSAGE = json.dumps({"event_type": "last_trade_price", "asset_id": "111"})

BOOK_EVENT_MISSING_ASSET_ID = json.dumps(
    {
        "event_type": "book",
        "bids": [{"price": "0.48", "size": "10"}],
        "asks": [{"price": "0.52", "size": "8"}],
        "timestamp": "1717000060000",
    }
)

BOOK_EVENT_MALFORMED_LEVEL = json.dumps(
    {
        "event_type": "book",
        "asset_id": "111",
        "bids": [{"price": "not-a-number", "size": "10"}],
        "asks": [{"price": "0.52", "size": "8"}],
        "timestamp": "1717000060000",
    }
)

PRICE_CHANGE_EVENT_EMPTY_LIST = json.dumps(
    {"event_type": "price_change", "timestamp": "1717000061000", "price_changes": []}
)


async def _run_single_message(message):
    """K-25 karsiligi testleri icin ortak kalip: bir mesaj gonder, client
    kapansin, sayaclari doner."""
    ws = FakeWebSocket([message])
    client = ClobMarketWSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    async def stop_soon():
        # Mesaj isleme asenkron -- bir tur event loop'a birak, sonra durdur.
        for _ in range(5):
            await asyncio.sleep(0)
        ws.closed_by_test = True
        client.stop()

    await asyncio.wait_for(asyncio.gather(client.run(), stop_soon()), timeout=5)
    return client


@pytest.mark.asyncio
async def test_not_json_message_counted_and_not_raised():
    client = await _run_single_message(NOT_JSON_MESSAGE)
    assert client.dropped_not_json == 1
    assert client.dropped_unknown_event_type == 0
    assert client.dropped_unknown_shape == 0


@pytest.mark.asyncio
async def test_unknown_event_type_counted():
    client = await _run_single_message(UNKNOWN_EVENT_TYPE_MESSAGE)
    assert client.dropped_unknown_event_type == 1
    assert client.dropped_not_json == 0
    assert client.dropped_unknown_shape == 0


@pytest.mark.asyncio
async def test_book_event_missing_asset_id_counted_as_unknown_shape():
    client = await _run_single_message(BOOK_EVENT_MISSING_ASSET_ID)
    assert client.dropped_unknown_shape == 1
    assert client.snapshot("111") is None


@pytest.mark.asyncio
async def test_book_event_malformed_price_counted_and_does_not_raise():
    """Eskiden `float("not-a-number")` yakalanmiyordu -- _handle_message
    patlar, PersistentWSClient'in genel except'ine dusup sahte bir
    'baglanti koptu' gibi goruniyordu. Simdi sayiliyor, baglanti kopmuyor."""
    client = await _run_single_message(BOOK_EVENT_MALFORMED_LEVEL)
    assert client.dropped_unknown_shape == 1
    assert client.snapshot("111") is None


@pytest.mark.asyncio
async def test_price_change_with_empty_list_counted_as_unknown_shape():
    client = await _run_single_message(PRICE_CHANGE_EVENT_EMPTY_LIST)
    assert client.dropped_unknown_shape == 1


@pytest.mark.asyncio
async def test_valid_book_and_price_change_do_not_increment_any_drop_counters():
    ws = FakeWebSocket([BOOK_EVENT, PRICE_CHANGE_EVENT])
    client = ClobMarketWSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    async def stop_soon():
        while client.snapshot("111") is None or client.snapshot("111")["venue_ts_ms"] != 1717000061000:
            await asyncio.sleep(0)
        ws.closed_by_test = True
        client.stop()

    await asyncio.wait_for(asyncio.gather(client.run(), stop_soon()), timeout=5)

    assert client.dropped_not_json == 0
    assert client.dropped_unknown_event_type == 0
    assert client.dropped_unknown_shape == 0


@pytest.mark.asyncio
async def test_resubscribes_on_reconnect():
    ws1 = FakeWebSocket([])
    ws2 = FakeWebSocket([])
    connections = [ws1, ws2]

    def connect_fn(url):
        return connections.pop(0)

    client = ClobMarketWSClient(connect_fn=connect_fn, sleep_fn=_instant_sleep)

    async def driver():
        while not client._ws_client._ws:
            await asyncio.sleep(0)
        await client.subscribe(["111"])
        ws1.closed_by_test = True  # ws1'i kopar, ws_client ws2'ye yeniden baglanmali
        while not ws2.sent:
            await asyncio.sleep(0)
        ws2.closed_by_test = True
        client.stop()

    await asyncio.wait_for(asyncio.gather(client.run(), driver()), timeout=5)
    assert json.loads(ws1.sent[0])["assets_ids"] == ["111"]
    assert json.loads(ws2.sent[0])["assets_ids"] == ["111"]
