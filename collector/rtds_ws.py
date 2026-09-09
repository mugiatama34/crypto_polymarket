"""RTDS WebSocket: crypto_prices (Binance relay) + crypto_prices_chainlink.

Kaynak: @polymarket/real-time-data-client (resmi npm paketi) README.md ve
src/model.ts -- bkz. collector/endpoints.py. Abonelik tek mesajla, iki
topic birden; zarf `{action, subscriptions}` sarmalayicisi GEREKLI --
`action` alani olmadan sunucu abonelik mesajini sessizce yok sayiyor,
hata donmuyor, baglanti acik kaliyor (bkz. docs/decisions.md K-27).

`filters` alani bir JSON STRING'dir (nesne degil) -- nesne gonderilirse
sunucu sessizce hic mesaj yollamiyor (Polymarket/rs-clob-client issue
#136 ile ayni belirti). Sembol formati topic'e gore FARKLI: `crypto_prices`
kucuk harfli "btcusdt" bekliyor, `crypto_prices_chainlink` kucuk harfli
ve egik cizgili "btc/usd" bekliyor -- bkz. collector/endpoints.py.

Zarfin `topic` alani AYIRT EDICI DEGIL: chainlink verisi de zarfta
`topic: "crypto_prices"` etiketiyle gelebiliyor (gozlendi, bkz.
docs/decisions.md K-28a). Hangi feed'e ait oldugu yalnizca
`payload.symbol`'den (`btcusdt`/`btc/usd`) belirlenebilir --
`_TOPIC_BY_SYMBOL` bunun icin kullanilir. `envelope.get("topic")`'e
guvenmek chainlink verisini sessizce binance cache'ine yazdirirdi.

Baglanti kurulunca sunucu "initial data dump" gonderiyor (README:
`{symbol, data: [...]}` sekli, `payload.value` yok) -- iki ayri prob
kosumunda (K-27, K-28) GOZLENEN TEK sekil bu; gercek tekil-guncelleme
cercevesi (`payload.value` dogrudan var) hic gorulmedi. Cache artik bu
dokum sekliyle DE doldurulur: `payload.data[]` icinden `timestamp`'i EN
BUYUK olan nokta secilir (korlemesine `[-1]` degil -- API siralamayi
garanti etmiyor, K-28c: chainlink dokumu duzensiz araliklarla geliyor).
Tekil-guncelleme seklinde bir cerceve bir gun gorulurse o da islenir.

Zarfin disindaki ust seviye `timestamp` YAYINCININ GONDERIM ZAMANIDIR
(`publish_ts_ms` olarak ayri saklanir), feed'in kendi gozlem zamani
DEGIL. Onceki varsayim -- feed zamaninin `payload.timestamp` icinde
oldugu -- iki ayri prob kosumunda da (K-27, K-28) hicbir cercevede
`payload.timestamp` gozlenmedigi icin curudu (bkz. docs/decisions.md
K-08 guncellemesi). Tek dogrulanmis feed-zamani kaynagi `payload.data[]`
icindeki nokta-bazli `timestamp`'lerdir: kullanildiginda
`feed_ts_source: "point"`; aksi halde (`data` yok, bos, veya kullanilabilir
nokta yok) `feed_ts_ms: None` ve `feed_ts_source: "none"` -- zarf
`timestamp`'ine ASLA dusulmez (publish ve feed zamanlari birbirinin
yerine gecmez, yanlis bir sifir staleness_ms uretilmesin diye).

Taninmayan cerceveler (JSON degil / sembol taninmiyor / ne kullanilabilir
`value` ne `data` noktasi var) sessizce dusuruluyordu (K-25 -- K-06'ya
aykiri). Tam duzeltme (ham cercevenin kaydi) hala acik; bu surumde en
azindan uc ayri sayac (`dropped_not_json`, `dropped_unknown_symbol`,
`dropped_unknown_shape`) tutulur ve `job_end` heartbeat'ine yazilir --
gorunurluk saglanir, dusurme davranisinin kendisi degismez.

Sessizlik korumasi iki ayri esikle calisir (bkz. docs/decisions.md K-23):
`silence_warn_sec` asilinca yalnizca bir alert kuyruguna yazilir (baglanti
korunur, `drain_alerts()` ile cagiran taraf -- runner.py -- bunu
heartbeat'e error olarak yazar). `silence_reconnect_sec` asilinca
baglanti zorla kapatilip yeniden kurulur. Esikler topic basina farkli
olabilir (chainlink/binance yayin kadansi farkli olabilir); varsayilanlar
gecicidir -- K-28b, 60 saniyelik bir pencerede SIFIR guncelleme cercevesi
gozlemledi (DOGRULANMADI, tek kosum); dogrulanirsa bu esikler production'da
surekli yanlis alarm uretiyor olabilir, ayri bir olcumle ele alinacak.
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
# K-28a: zarfin `topic` alani ayirt edici degil -- gercek eslestirme
# payload.symbol uzerinden, `_SYMBOL_BY_TOPIC`'in tersiyle yapilir.
_TOPIC_BY_SYMBOL = {symbol: topic for topic, symbol in _SYMBOL_BY_TOPIC.items()}

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


def _select_latest_point(data: list) -> Optional[dict]:
    """`payload.data[]` icinden `timestamp`'i EN BUYUK olan, hem
    `timestamp` hem `value` tasiyan noktayi secer -- korlemesine `[-1]`
    degil, API siralamayi garanti etmiyor (bkz. modul docstring'i,
    K-28c). Uygun nokta yoksa `None`."""
    candidates = [
        point
        for point in data
        if isinstance(point, dict) and "timestamp" in point and "value" in point
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda point: point["timestamp"])


def _extract_price(payload: dict) -> Optional[tuple]:
    """`payload`'dan `(value, feed_ts_ms, feed_ts_source)` cikarir.

    Iki sekil: `data[]` dokumu (TEK gozlenen sekil, bkz. K-27/K-28b) veya
    dogrudan `value` (hic gozlenmedi, ama uretim API'si varsayimsal
    olarak destekliyor). Ikisi de kullanilamazsa `None` doner (cagiran
    `dropped_unknown_shape` sayar).

    `feed_ts_ms` yalnizca `data[]`'dan secilen bir noktadan gelir --
    zarf `timestamp`'i (publish zamani) hicbir zaman feed_ts olarak
    kullanilmaz (bkz. modul docstring'i, K-08 guncellemesi)."""
    data = payload.get("data")
    if isinstance(data, list) and data:
        point = _select_latest_point(data)
        if point is None:
            return None
        return float(point["value"]), int(point["timestamp"]), "point"

    if "value" in payload:
        value = payload.get("value")
        return (float(value) if value is not None else None), None, "none"

    return None


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
        # K-25: sessizce dusen cerceveler icin gorunurluk (tam duzeltme --
        # ham cerceve kaydi -- hala acik, bkz. modul docstring'i).
        self.dropped_not_json = 0
        self.dropped_unknown_symbol = 0
        self.dropped_unknown_shape = 0
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
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": topic,
                    "type": RTDS_SUBSCRIPTION_TYPE,
                    "filters": json.dumps({"symbol": _SYMBOL_BY_TOPIC[topic]}),
                }
                for topic in TOPICS
            ],
        }
        await ws.send(json.dumps(subscription))

    async def _handle_message(self, raw_message: str) -> None:
        try:
            envelope = json.loads(raw_message)
        except (json.JSONDecodeError, TypeError):
            self.dropped_not_json += 1
            return

        payload = envelope.get("payload")
        symbol = payload.get("symbol") if isinstance(payload, dict) else None
        topic = _TOPIC_BY_SYMBOL.get(symbol)
        if topic is None:
            self.dropped_unknown_symbol += 1
            return

        extracted = _extract_price(payload)
        if extracted is None:
            self.dropped_unknown_shape += 1
            return
        value, feed_ts_ms, feed_ts_source = extracted

        publish_ts = envelope.get("timestamp")
        self.cache[topic] = {
            "value": value,
            "feed_ts_ms": feed_ts_ms,
            "feed_ts_source": feed_ts_source,
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
