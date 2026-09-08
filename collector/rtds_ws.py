"""RTDS WebSocket: crypto_prices (Binance relay) + crypto_prices_chainlink.

Kaynak: @polymarket/real-time-data-client (resmi npm paketi) README.md ve
src/model.ts -- bkz. collector/endpoints.py. Abonelik tek mesajla, iki
topic birden; her mesaj zarfi `{topic, type, timestamp, payload,
connection_id}`, payload (CryptoPrice) `{symbol, timestamp(ms), value}`.

`filters` alani bir JSON STRING'dir (nesne degil) -- nesne gonderilirse
sunucu sessizce hic mesaj yollamiyor (Polymarket/rs-clob-client issue
#136 ile ayni belirti). Sembol formati topic'e gore FARKLI: `crypto_prices`
kucuk harfli "btcusdt" bekliyor, `crypto_prices_chainlink` kucuk harfli
ve egik cizgili "btc/usd" bekliyor -- bkz. collector/endpoints.py.

Baglanti kurulunca sunucu "initial data dump" da gonderebiliyor (README:
`{symbol, data: [...]}` sekli) -- bu mesajlarda `payload.value` yok, bu
yuzden atlaniyor; yalnizca tekil guncellemeler ("value" alani olan)
cache'e yazilir.

Zarfin disindaki ust seviye `timestamp` YAYINCININ GONDERIM ZAMANIDIR,
Chainlink'in kendi gozlem zamani degil -- o `payload.timestamp` icinde
(`feed_ts_ms`). Ikisi karistirilmaz; disaridaki `publish_ts_ms` olarak
ayri saklanir (bkz. docs/decisions.md K-22 sonrasi tartisma).

Sessizlik korumasi iki ayri esikle calisir (bkz. docs/decisions.md K-23):
`silence_warn_sec` asilinca yalnizca bir alert kuyruguna yazilir (baglanti
korunur, `drain_alerts()` ile cagiran taraf -- runner.py -- bunu
heartbeat'e error olarak yazar). `silence_reconnect_sec` asilinca
baglanti zorla kapatilip yeniden kurulur. Esikler topic basina farkli
olabilir (chainlink/binance yayin kadansi farkli olabilir); varsayilanlar
gecicidir, gercek mesajlar-arasi gecikme dagilimi olculmeden secildi.
"""

import asyncio
import contextlib
import json
import time
from typing import Callable, Optional

from .endpoints import (
    RTDS_PING_INTERVAL_SEC,
    RTDS_PING_MESSAGE,
    RTDS_SUBSCRIPTION_TYPE,
    RTDS_SYMBOL_BINANCE,
    RTDS_SYMBOL_CHAINLINK,
    RTDS_TOPIC_BINANCE,
    RTDS_TOPIC_CHAINLINK,
    RTDS_WS_URL,
)
from .ws_client import PersistentWSClient

TOPICS = (RTDS_TOPIC_BINANCE, RTDS_TOPIC_CHAINLINK)

_SYMBOL_BY_TOPIC = {
    RTDS_TOPIC_BINANCE: RTDS_SYMBOL_BINANCE,
    RTDS_TOPIC_CHAINLINK: RTDS_SYMBOL_CHAINLINK,
}

# K-23: gecici varsayilanlar -- olculmus mesajlar-arasi gecikme
# dagilimiyla degistirilecek (bkz. scripts/probe.py gap-dagilimi problari).
DEFAULT_SILENCE_WARN_SEC = 30.0
DEFAULT_SILENCE_RECONNECT_SEC = 120.0
_WATCHDOG_POLL_SEC = 5.0


def _per_topic(value, default: float) -> dict:
    """`value` None ise her topic `default`; tek sayi ise her topic o
    sayi; `{topic: sayi}` sozlugu ise topic basina, eksik olanlar
    `default`'a duser (bkz. modul docstring'i -- topic basina ayarlanabilir)."""
    if value is None:
        return {topic: default for topic in TOPICS}
    if isinstance(value, dict):
        return {topic: value.get(topic, default) for topic in TOPICS}
    return {topic: value for topic in TOPICS}


class RTDSClient:
    def __init__(
        self,
        *,
        connect_fn=None,
        now_ms_fn=None,
        sleep_fn=None,
        on_disconnect=None,
        silence_warn_sec=None,
        silence_reconnect_sec=None,
    ):
        self.cache: dict = {topic: None for topic in TOPICS}
        self._now_ms = now_ms_fn or (lambda: int(time.time() * 1000))
        self._sleep = sleep_fn or asyncio.sleep
        self._silence_warn_sec = _per_topic(silence_warn_sec, DEFAULT_SILENCE_WARN_SEC)
        self._silence_reconnect_sec = _per_topic(silence_reconnect_sec, DEFAULT_SILENCE_RECONNECT_SEC)
        self._last_data_ms: dict = {topic: None for topic in TOPICS}
        self._warned: dict = {topic: False for topic in TOPICS}
        self._alerts: list = []
        self._watchdog_task = None
        self._ws_client = PersistentWSClient(
            RTDS_WS_URL,
            on_message=self._handle_message,
            on_open=self._handle_open,
            on_disconnect=on_disconnect,
            connect_fn=connect_fn,
            ping_interval_sec=RTDS_PING_INTERVAL_SEC,
            ping_message=RTDS_PING_MESSAGE,
            now_ms_fn=now_ms_fn,
            sleep_fn=sleep_fn,
        )

    async def run(self) -> None:
        self._watchdog_task = asyncio.create_task(self._watchdog_loop())
        try:
            await self._ws_client.run()
        finally:
            self._watchdog_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._watchdog_task
            self._watchdog_task = None

    def stop(self) -> None:
        self._ws_client.stop()

    def set_on_disconnect(self, callback) -> None:
        self._ws_client.set_on_disconnect(callback)

    async def _handle_open(self, ws) -> None:
        now = self._now_ms()
        for topic in TOPICS:
            self._last_data_ms[topic] = now
            self._warned[topic] = False
        subscription = {
            "subscriptions": [
                {
                    "topic": topic,
                    "type": RTDS_SUBSCRIPTION_TYPE,
                    "filters": json.dumps({"symbol": _SYMBOL_BY_TOPIC[topic]}),
                }
                for topic in TOPICS
            ]
        }
        await ws.send(json.dumps(subscription))

    async def _handle_message(self, raw_message: str) -> None:
        try:
            envelope = json.loads(raw_message)
        except (json.JSONDecodeError, TypeError):
            return

        topic = envelope.get("topic")
        if topic not in self.cache:
            return

        payload = envelope.get("payload")
        if not isinstance(payload, dict) or "value" not in payload:
            return  # initial data dump veya taninmayan sekil

        value = payload.get("value")
        feed_ts = payload.get("timestamp")
        publish_ts = envelope.get("timestamp")
        self.cache[topic] = {
            "value": float(value) if value is not None else None,
            "feed_ts_ms": int(feed_ts) if feed_ts is not None else None,
            "publish_ts_ms": int(publish_ts) if publish_ts is not None else None,
            "raw_envelope": envelope,
        }
        self._last_data_ms[topic] = self._now_ms()
        self._warned[topic] = False

    def snapshot(self, topic: str) -> Optional[dict]:
        return self.cache.get(topic)

    def drain_alerts(self) -> list:
        """Bekleyen sessizlik uyarilarini dondurur ve kuyruktan siler.
        Runner her tick'te bunu heartbeat.error'a yazar -- bu client'in
        kendisi heartbeat'e erisemez (bkz. docs/decisions.md K-23)."""
        alerts, self._alerts = self._alerts, []
        return alerts

    async def _watchdog_loop(self) -> None:
        while True:
            await self._sleep(_WATCHDOG_POLL_SEC)
            now = self._now_ms()
            for topic in TOPICS:
                await self._check_topic_silence(topic, now)

    async def _check_topic_silence(self, topic: str, now: int) -> None:
        """Tek bir topic icin sessizlik kontrolu -- `_watchdog_loop`'tan
        ayri metod olarak tutulur ki testler gercek zaman/event-loop
        yarisina girmeden dogrudan cagirabilsin (bkz. tests/test_rtds_ws.py)."""
        last = self._last_data_ms[topic]
        if last is None:
            return
        silence_sec = (now - last) / 1000.0
        reconnect_after = self._silence_reconnect_sec[topic]
        warn_after = self._silence_warn_sec[topic]

        if silence_sec >= reconnect_after:
            self._alerts.append(
                f"RTDS sessizlik: {topic} icin {silence_sec:.0f}s mesaj yok, yeniden baglaniliyor"
            )
            self._last_data_ms[topic] = now
            self._warned[topic] = False
            await self._ws_client.force_reconnect(f"rtds_silence:{topic}")
        elif silence_sec >= warn_after and not self._warned[topic]:
            self._warned[topic] = True
            self._alerts.append(f"RTDS sessizlik uyarisi: {topic} icin {silence_sec:.0f}s mesaj yok")
