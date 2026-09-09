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


async def _run_until(client, ws, predicate, timeout=5):
    async def stop_soon():
        while not predicate():
            await asyncio.sleep(0)
        ws.closed_by_test = True
        client.stop()

    await asyncio.wait_for(asyncio.gather(client.run(), stop_soon()), timeout=timeout)


def _dump_envelope(topic, symbol, points, publish_ts=999999):
    """Iki prob kosumunda (K-27, K-28) GOZLENEN TEK sekil: initial data
    dump. `topic` kasitli olarak sembolden BAGIMSIZ verilebilir -- K-28a
    regresyon testi icin (zarftaki topic alani ayirt edici degil)."""
    return json.dumps(
        {
            "topic": topic,
            "type": "subscribe",
            "timestamp": publish_ts,
            "payload": {"symbol": symbol, "data": points},
        }
    )


def _update_envelope(topic, symbol, value, payload_timestamp, publish_ts=None):
    """Hic gozlenmemis, varsayimsal tekil-guncelleme sekli -- uretim
    API'si destekliyor olabilir ama iki prob kosumunda da gorulmedi
    (bkz. docs/decisions.md K-27/K-28b). `payload_timestamp` KASITLI
    OLARAK YOK SAYILIR (K-08 guncellemesi -- payload.timestamp varsayimi
    curudu): feed_ts_ms bu sekilde her zaman None kalir."""
    if publish_ts is None:
        publish_ts = payload_timestamp + 5
    return json.dumps(
        {
            "topic": topic,
            "type": "update",
            "timestamp": publish_ts,
            "payload": {"symbol": symbol, "timestamp": payload_timestamp, "value": value},
        }
    )


@pytest.mark.asyncio
async def test_subscribes_to_both_topics_with_action_field_on_open():
    """K-27: action alani olmadan sunucu abonelik mesajini sessizce yok
    sayiyordu -- bkz. docs/decisions.md K-27."""
    ws = FakeWebSocket([])
    client = RTDSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    await _run_until(client, ws, lambda: bool(ws.sent))

    sent = json.loads(ws.sent[0])
    assert sent["action"] == "subscribe"
    topics = {s["topic"] for s in sent["subscriptions"]}
    assert topics == {"crypto_prices", "crypto_prices_chainlink"}
    # Sembol formati topic'e gore FARKLI -- bkz. docs/decisions.md,
    # collector/endpoints.py RTDS_SYMBOL_BINANCE/RTDS_SYMBOL_CHAINLINK.
    expected_symbol = {"crypto_prices": "btcusdt", "crypto_prices_chainlink": "btc/usd"}
    for sub in sent["subscriptions"]:
        assert isinstance(sub["filters"], str)  # nesne degil, JSON string
        assert json.loads(sub["filters"]) == {"symbol": expected_symbol[sub["topic"]]}


@pytest.mark.asyncio
async def test_processes_initial_dump_selecting_max_timestamp_point():
    """TEK gozlenen sekil (K-27/K-28b). Nokta dizisi KASITLI OLARAK
    siralanmamis -- korlemesine [-1] alinirsa yanlis nokta secilir
    (bkz. docs/decisions.md K-28c: chainlink dokumu duzensiz araliklarla
    geliyor, siralama garanti degil)."""
    points = [
        {"timestamp": 1000, "value": 100.0},
        {"timestamp": 3000, "value": 300.0},  # en buyuk timestamp, ama son eleman degil
        {"timestamp": 2000, "value": 200.0},
    ]
    dump = _dump_envelope("crypto_prices", "btcusdt", points, publish_ts=5000)
    ws = FakeWebSocket([dump])
    client = RTDSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    await _run_until(client, ws, lambda: client.snapshot("crypto_prices") is not None)

    snap = client.snapshot("crypto_prices")
    assert snap["value"] == 300.0
    assert snap["feed_ts_ms"] == 3000
    assert snap["feed_ts_source"] == "point"
    assert snap["publish_ts_ms"] == 5000


@pytest.mark.asyncio
async def test_topic_routing_uses_payload_symbol_not_envelope_topic():
    """K-28a regresyon: zarftaki `topic` alani "crypto_prices" olsa bile
    payload.symbol "btc/usd" ise veri chainlink cache'ine yazilmali --
    gercek prob verisinde gozlenen tam olarak bu (bkz.
    probe_output/20260909T082742Z, docs/decisions.md K-28a)."""
    mislabeled = _dump_envelope(
        "crypto_prices",  # yanlis etiket, gercek RTDS davranisi
        "btc/usd",
        [{"timestamp": 1000, "value": 79593.3}],
    )
    ws = FakeWebSocket([mislabeled])
    client = RTDSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    await _run_until(client, ws, lambda: client.snapshot("crypto_prices_chainlink") is not None)

    assert client.snapshot("crypto_prices_chainlink")["value"] == 79593.3
    assert client.snapshot("crypto_prices") is None


@pytest.mark.asyncio
async def test_update_shape_never_trusts_payload_timestamp():
    """K-08 guncellemesi: payload.timestamp'in feed'in kendi gozlem
    zamani oldugu varsayimi iki prob kosumunda da (K-27, K-28) curudu --
    tekil-guncelleme sekli gorulurse bile feed_ts_ms None kalir."""
    update = _update_envelope("crypto_prices", "btcusdt", 67000.5, payload_timestamp=1717000060000)
    ws = FakeWebSocket([update])
    client = RTDSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    await _run_until(client, ws, lambda: client.snapshot("crypto_prices") is not None)

    snap = client.snapshot("crypto_prices")
    assert snap["value"] == 67000.5
    assert snap["feed_ts_ms"] is None
    assert snap["feed_ts_source"] == "none"
    # publish_ts_ms zarf timestamp'inden dolar, feed_ts_ms'ten AYRI --
    # ikisi asla karistirilmaz (bkz. modul docstring'i).
    assert snap["publish_ts_ms"] == 1717000060005
    assert snap["raw_envelope"]["topic"] == "crypto_prices"


@pytest.mark.asyncio
async def test_dropped_not_json_counter_increments_on_bad_json():
    ws = FakeWebSocket(["not json {{{"])
    client = RTDSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    await _run_until(client, ws, lambda: client.dropped_not_json >= 1)

    assert client.dropped_not_json == 1
    assert client.dropped_unknown_symbol == 0
    assert client.dropped_unknown_shape == 0


@pytest.mark.asyncio
async def test_dropped_unknown_symbol_counter_increments_on_unrecognized_symbol():
    other = json.dumps({"topic": "comments", "payload": {"symbol": "ethusdt", "value": 1}})
    ws = FakeWebSocket([other])
    client = RTDSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    await _run_until(client, ws, lambda: client.dropped_unknown_symbol >= 1)

    assert client.dropped_unknown_symbol == 1
    assert client.snapshot("crypto_prices") is None


@pytest.mark.asyncio
async def test_dropped_unknown_symbol_counter_increments_when_payload_missing():
    other = json.dumps({"topic": "crypto_prices"})
    ws = FakeWebSocket([other])
    client = RTDSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    await _run_until(client, ws, lambda: client.dropped_unknown_symbol >= 1)

    assert client.dropped_unknown_symbol == 1


@pytest.mark.asyncio
async def test_dropped_unknown_shape_counter_increments_when_no_usable_value_or_data():
    envelope = json.dumps({"topic": "crypto_prices", "payload": {"symbol": "btcusdt"}})
    ws = FakeWebSocket([envelope])
    client = RTDSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    await _run_until(client, ws, lambda: client.dropped_unknown_shape >= 1)

    assert client.dropped_unknown_shape == 1
    assert client.snapshot("crypto_prices") is None


@pytest.mark.asyncio
async def test_dropped_unknown_shape_counter_increments_on_empty_data_array():
    envelope = json.dumps({"topic": "crypto_prices", "payload": {"symbol": "btcusdt", "data": []}})
    ws = FakeWebSocket([envelope])
    client = RTDSClient(connect_fn=lambda url: ws, sleep_fn=_instant_sleep)

    await _run_until(client, ws, lambda: client.dropped_unknown_shape >= 1)

    assert client.dropped_unknown_shape == 1


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
