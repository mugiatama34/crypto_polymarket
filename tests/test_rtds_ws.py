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

    async def close(self):
        self.closed_by_test = True


async def _instant_sleep(_seconds):
    await asyncio.sleep(0)


def _envelope(topic, value, feed_ts, publish_ts=None):
    """`publish_ts` (zarf disi -- yayincinin gonderim zamani) `feed_ts`ten
    (payload icinde -- Chainlink'in kendi gozlem zamani) BILEREK farkli
    verilebilir; ikisi karistirilmamali (bkz. collector/rtds_ws.py)."""
    if publish_ts is None:
        publish_ts = feed_ts + 5  # varsayilan: kasitli farkli, karistirma hatasini yakalar
    return json.dumps(
        {
            "topic": topic,
            "type": "update",
            "timestamp": publish_ts,
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
    # Sembol formati topic'e gore FARKLI -- bkz. docs/decisions.md,
    # collector/endpoints.py RTDS_SYMBOL_BINANCE/RTDS_SYMBOL_CHAINLINK.
    expected_symbol = {"crypto_prices": "btcusdt", "crypto_prices_chainlink": "btc/usd"}
    for sub in sent["subscriptions"]:
        assert isinstance(sub["filters"], str)  # nesne degil, JSON string
        assert json.loads(sub["filters"]) == {"symbol": expected_symbol[sub["topic"]]}


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
    assert binance["value"] == 67000.5
    assert binance["feed_ts_ms"] == 1717000060000
    # publish_ts_ms (zarf disi -- yayincinin gonderim zamani) feed_ts_ms'ten
    # (payload icinde -- Chainlink gozlem zamani) AYRI tutulur, ikisi
    # karistirilmaz (bkz. modul docstring'i).
    assert binance["publish_ts_ms"] == 1717000060005
    assert chainlink["value"] == 66998.1
    assert chainlink["feed_ts_ms"] == 1717000059800
    assert chainlink["publish_ts_ms"] == 1717000059805
    # Ham zarf da saklanir -- raw[] icin (bkz. collector/sampler.py).
    assert binance["raw_envelope"]["topic"] == "crypto_prices"
    assert binance["raw_envelope"]["payload"]["value"] == 67000.5


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


@pytest.mark.asyncio
async def test_ping_sent_is_uppercase():
    """Uretim istemcisi PersistentWSClient uzerinden RTDS_PING_MESSAGE'i
    gonderir; burada dogrudan RTDSClient'in bu sabiti PersistentWSClient'e
    dogru gectigini kontrol ediyoruz (gercek gonderim PersistentWSClient
    testlerinde -- tests/test_ws_client.py)."""
    ws = FakeWebSocket(["x"])
    client = RTDSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)
    assert client._ws_client._ping_message == "PING"


class _RecordingWSClient:
    """PersistentWSClient'i taklit eder -- yalnizca force_reconnect
    cagrilarini kaydeder, gercek baglanti/loop yok."""

    def __init__(self):
        self.force_reconnect_calls = []

    async def force_reconnect(self, reason=None):
        self.force_reconnect_calls.append(reason)


@pytest.mark.asyncio
async def test_silence_warn_alerts_without_reconnecting():
    client = RTDSClient(silence_warn_sec=30.0, silence_reconnect_sec=120.0)
    client._ws_client = _RecordingWSClient()
    client._last_data_ms["crypto_prices_chainlink"] = 0

    await client._check_topic_silence("crypto_prices_chainlink", now=30_000)

    alerts = client.drain_alerts()
    assert len(alerts) == 1
    assert "sessizlik uyarisi" in alerts[0]
    assert "crypto_prices_chainlink" in alerts[0]
    assert client._ws_client.force_reconnect_calls == []
    # Ayni esik tekrar asilirsa (henuz reconnect esigine gelmeden) ikinci
    # kez uyarmaz -- coverage tek satirlik gurultuye bogulmasin.
    await client._check_topic_silence("crypto_prices_chainlink", now=31_000)
    assert client.drain_alerts() == []


@pytest.mark.asyncio
async def test_silence_reconnect_forces_reconnect_and_alerts():
    client = RTDSClient(silence_warn_sec=30.0, silence_reconnect_sec=120.0)
    client._ws_client = _RecordingWSClient()
    client._last_data_ms["crypto_prices"] = 0

    await client._check_topic_silence("crypto_prices", now=120_000)

    alerts = client.drain_alerts()
    assert len(alerts) == 1
    assert "yeniden baglaniliyor" in alerts[0]
    assert client._ws_client.force_reconnect_calls == ["rtds_silence:crypto_prices"]


@pytest.mark.asyncio
async def test_silence_thresholds_are_per_topic():
    client = RTDSClient(
        silence_warn_sec={"crypto_prices": 10.0, "crypto_prices_chainlink": 30.0},
        silence_reconnect_sec=120.0,
    )
    client._ws_client = _RecordingWSClient()
    client._last_data_ms["crypto_prices"] = 0
    client._last_data_ms["crypto_prices_chainlink"] = 0

    # 15s: binance esigini (10s) gecti, chainlink'inkini (30s) gecmedi.
    await client._check_topic_silence("crypto_prices", now=15_000)
    await client._check_topic_silence("crypto_prices_chainlink", now=15_000)

    alerts = client.drain_alerts()
    assert len(alerts) == 1
    assert "crypto_prices" in alerts[0] and "chainlink" not in alerts[0]
