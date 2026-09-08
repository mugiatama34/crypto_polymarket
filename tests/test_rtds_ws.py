import asyncio
import json

import pytest

from collector.rtds_ws import RTDSClient


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


def _envelope(topic, value, feed_ts):
    return json.dumps(
        {
            "topic": topic,
            "type": "update",
            "timestamp": feed_ts,
            "connection_id": "conn-1",
            "payload": {"symbol": "BTCUSDT", "timestamp": feed_ts, "value": value},
        }
    )


@pytest.mark.asyncio
async def test_subscribes_to_both_topics_on_open():
    ws = FakeWebSocket([])
    client = RTDSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    async def stop_soon():
        while not ws.sent:
            await asyncio.sleep(0)
        ws.closed_by_test = True
        client.stop()

    await asyncio.wait_for(asyncio.gather(client.run(), stop_soon()), timeout=5)

    sent = json.loads(ws.sent[0])
    topics = {s["topic"] for s in sent["subscriptions"]}
    assert topics == {"crypto_prices", "crypto_prices_chainlink"}
    for sub in sent["subscriptions"]:
        assert json.loads(sub["filters"]) == {"symbol": "BTCUSDT"}


@pytest.mark.asyncio
async def test_caches_price_updates_per_topic():
    messages = [
        _envelope("crypto_prices", 67000.5, 1717000060000),
        _envelope("crypto_prices_chainlink", 66998.1, 1717000059800),
    ]
    ws = FakeWebSocket(messages)
    client = RTDSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    async def stop_soon():
        while client.snapshot("crypto_prices_chainlink") is None:
            await asyncio.sleep(0)
        ws.closed_by_test = True
        client.stop()

    await asyncio.wait_for(asyncio.gather(client.run(), stop_soon()), timeout=5)

    binance = client.snapshot("crypto_prices")
    chainlink = client.snapshot("crypto_prices_chainlink")
    assert binance == {"value": 67000.5, "feed_ts_ms": 1717000060000}
    assert chainlink == {"value": 66998.1, "feed_ts_ms": 1717000059800}


@pytest.mark.asyncio
async def test_ignores_initial_data_dump_without_value():
    dump = json.dumps(
        {
            "topic": "crypto_prices",
            "type": "update",
            "payload": {"symbol": "BTCUSDT", "data": [{"timestamp": 1, "value": 1.0}]},
        }
    )
    ws = FakeWebSocket([dump])
    client = RTDSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    async def stop_soon():
        for _ in range(50):
            await asyncio.sleep(0)
        ws.closed_by_test = True
        client.stop()

    await asyncio.wait_for(asyncio.gather(client.run(), stop_soon()), timeout=5)
    assert client.snapshot("crypto_prices") is None


@pytest.mark.asyncio
async def test_ignores_unknown_topic():
    other = json.dumps({"topic": "comments", "payload": {"value": 1}})
    ws = FakeWebSocket([other])
    client = RTDSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    async def stop_soon():
        for _ in range(50):
            await asyncio.sleep(0)
        ws.closed_by_test = True
        client.stop()

    await asyncio.wait_for(asyncio.gather(client.run(), stop_soon()), timeout=5)
    assert client.snapshot("crypto_prices") is None
