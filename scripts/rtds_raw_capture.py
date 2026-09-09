#!/usr/bin/env python3
"""Tek seferlik izolasyon probu: RTDS websocket'ine baglanip HICBIR
ayristirma/filtreleme yapmadan ham cerceveleri kaydeder.

Neden: `scripts/probe.py`'deki `_probe_rtds` ve
`_probe_rtds_gap_distribution` ikisi de mesaj filtreliyor --
`_probe_rtds` JSON parse edip listeye ekliyor (bu, ham degil), gap
dagilimi ise topic + `payload.value` seklini bekleyen mesajlari sayiyor.
Ikisi de son kosumda (bkz. probe_output/20260908T214511Z)
message_count/count: 0 gosterdi. Bu script o filtrelerin HICBIRINI
uygulamaz -- gelen her cerceveyi (JSON olsun olmasin, hangi topic'e ait
olursa olsun) oldugu gibi ham string olarak yazar. Amac: "sunucu hic
mesaj yollamadi" ile "mesaj geldi ama taninmayan sekilde oldugu icin
onceki problarda sayilmadi" ayrimini yapabilmek.

`collector/rtds_ws.py._handle_message`'in ayni sinifta uc sessiz
`return`'u var (JSON degilse, topic taninmiyorsa, payload.value yoksa)
-- K-06'ya aykiri bir bug, ama bu probun konusu DEGIL ve bu dosyada
DUZELTILMEDI (bkz. docs/decisions.md K-25): `_probe_rtds` hicbir sey
filtrelemedigi halde de sifir gosterdi, yani sorun secicilikte degil.

UC asama, TEK baglanti uzerinden:

  1) Baglan, HICBIR abonelik gonderme, 15 saniye dinle -- sunucu
     kendiliginden bir sey yolluyor mu (welcome/ack/initial dump)?
  2) `crypto_prices`'a FILTRESIZ abone ol -- `filters` alani hic
     gonderilmez (gorev tanimindaki varsayim: filtre atlanirsa tum
     semboller gelir). 30 saniye dinle.
  3) TAMAMEN FARKLI, crypto-disi aday topic'lere (PHASE3_CANDIDATE_TOPICS)
     filtresiz abone ol, 30 saniye dinle. Amac: sessizligin crypto
     topic'lerine mi ozgu oldugunu, yoksa abonelik mekanizmasinin
     tamaminin mi sessiz oldugunu ayirt etmek. Bu topic adlari ADAY --
     RTDS'in resmi paket dokumaninda bu depoda dogrulanmadi, gorev
     tanimindaki ornek isimler kullanildi (bkz. asagidaki sabit). Hicbiri
     mesaj getirmezse bu tek basina KANIT SAYILMAZ (topic adlari yanlis
     olabilir) -- yalnizca en az biri mesaj getirirse "crypto'ya ozgu"
     sonucu netlesir.

El sikisma ayrintisi (HTTP durum kodu, response header'lari, secilen
subprotocol) baglanti kurulur kurulmaz kaydedilir -- "connected: true"
101 disinda bir sonuc gizleyebilir mi sorusuna cevap icin. Legacy
istemci (websockets.legacy.client) yalnizca 101'de basariyla donuyor,
aksi halde `InvalidStatusCode` firlatiyor (bkz. `handshake()` kaynagi) --
o yuzden basari durumunda durum kodu dogrudan olculmuyor, CIKARIM
olarak isaretleniyor (`status_code_source`); basarisizlikta ise
istisnadan dogrudan okunuyor.

PING gonderimleri de ayni kronolojik olay listesine yazilir (uretim
istemcisiyle -- collector/ws_client.py PersistentWSClient -- ayni
kadans ve mesaj: RTDS_PING_INTERVAL_SEC'te bir RTDS_PING_MESSAGE) --
boylece bir PING'den hemen sonra `frame_received` gelip gelmedigi
(=pong benzeri bir yanit) zaman damgasindan incelenebilir; icerik
yorumlanmaz. WS kapanma kodu ve sebebi ayrica kaydedilir.

Bu prob HICBIR SEYI DOGRULAMAZ (bkz. scripts/probe.py docstring'i,
ayni ilke): validator/schemas'tan gecmez, collector/ ile ilgisi yoktur,
uretim koduna baglanmaz.

Kullanim:
    python -m scripts.rtds_raw_capture

Cikti: probe_output/<UTC-ISO-zaman>/rtds_raw_capture.json
"""

import asyncio
import contextlib
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import websockets
import websockets.exceptions

from collector.endpoints import (
    RTDS_PING_INTERVAL_SEC,
    RTDS_PING_MESSAGE,
    RTDS_SUBSCRIPTION_TYPE,
    RTDS_TOPIC_BINANCE,
    RTDS_WS_URL,
)
from scripts.probe import _write_json

PHASE1_LISTEN_SECONDS = 15.0
PHASE2_LISTEN_SECONDS = 30.0
PHASE3_LISTEN_SECONDS = 30.0

# ADAY, DOGRULANMAMIS: RTDS'in resmi paket dokumaninda (README/model.ts)
# crypto-disi hangi topic'lerin var oldugu bu depoda hic dogrulanmadi.
# Gorev tanimindaki ornek isimler kullanildi. Abonelik tipi de bu
# topic'ler icin bilinmiyor -- "update" yerine "*" denendi (scripts/probe.py
# _probe_rtds_type_fallback'te chainlink icin zaten kullanilan, "hepsini
# dinle" anlamina gelen ayni tip). Ikisi de sessiz kalirsa bu, topic
# adlarinin yanlis olma ihtimalini DISLAMAZ -- yalnizca biri mesaj
# getirirse "abonelik mekanizmasi genel olarak calisiyor, sorun crypto'ya
# ozgu" sonucuna varilabilir.
PHASE3_CANDIDATE_TOPICS = ("activity", "comments")
PHASE3_SUBSCRIPTION_TYPE = "*"


def _now_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _event(kind: str, monotonic_start: float, **fields) -> dict:
    return {
        "kind": kind,
        "wall_clock_utc": datetime.now(timezone.utc).isoformat(),
        "elapsed_sec": round(time.monotonic() - monotonic_start, 3),
        **fields,
    }


def _subscription_message(topics: tuple, sub_type: str) -> str:
    """`filters` alani HIC gonderilmez -- uretim istemcisi
    (collector/rtds_ws.py) her zaman bir `filters` JSON string'i
    gonderiyor; bu, gorev tanimindaki varsayimi (filtre atlanirsa tum
    semboller gelir) test etmek icin o varsayimdan kasitli sapan bir
    izolasyon denemesidir."""
    return json.dumps(
        {"subscriptions": [{"topic": topic, "type": sub_type} for topic in topics]}
    )


def _handshake_info(ws) -> dict:
    """Basarili baglanmadan hemen sonra el sikisma ayrintisi. Legacy
    istemci (websockets.legacy.client.WebSocketClientProtocol.handshake)
    yalnizca HTTP durum kodu 101 ise basariyla donuyor, aksi halde
    `InvalidStatusCode` firlatiyor -- bu yuzden basari durumunda 101
    dogrudan olculmus degil, CIKARIM. Header'lar/subprotocol dogrudan
    baglanti nesnesinden okunur."""
    response_headers = getattr(ws, "response_headers", None)
    return {
        "status_code": 101,
        "status_code_source": "inferred_from_successful_connect",
        "response_headers": list(response_headers.raw_items()) if response_headers is not None else None,
        "subprotocol": getattr(ws, "subprotocol", None),
    }


def _handshake_failure_info(exc: websockets.exceptions.InvalidStatusCode) -> dict:
    """101 disinda bir HTTP yaniti geldiginde (legacy istemci baglanmayi
    tamamlamadan `InvalidStatusCode` firlatir) -- bu durumda durum kodu
    OLCULMUS, cikarim degil."""
    headers = getattr(exc, "headers", None)
    return {
        "status_code": getattr(exc, "status_code", None),
        "status_code_source": "observed_handshake_failure",
        "response_headers": list(headers.raw_items()) if headers is not None else None,
        "subprotocol": None,
    }


async def _ping_loop(ws, events: list, monotonic_start: float, deadline: float) -> None:
    """Uretim istemcisiyle (collector/ws_client.py PersistentWSClient)
    ayni kadans: RTDS_PING_INTERVAL_SEC'te bir RTDS_PING_MESSAGE
    gonderir, gonderim anini olay listesine yazar."""
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        await asyncio.sleep(min(RTDS_PING_INTERVAL_SEC, remaining))
        if time.monotonic() >= deadline:
            return
        with contextlib.suppress(Exception):
            await ws.send(RTDS_PING_MESSAGE)
            events.append(_event("ping_sent", monotonic_start, message=RTDS_PING_MESSAGE))


async def _listen_raw(ws, events: list, monotonic_start: float, deadline: float, *, send_pings: bool) -> None:
    """`deadline`e (time.monotonic()) kadar gelen HER cerceveyi -- JSON
    olsun olmasin, hangi topic'e ait olursa olsun -- oldugu gibi ham
    string olarak `events`e ekler. Ayristirma/filtreleme yok (bkz. modul
    docstring'i). `ConnectionClosed` burada YAKALANMAZ -- cagiran
    (`_run`) yakalar, kapanma kodu/sebebini oradan alir."""
    ping_task = asyncio.create_task(_ping_loop(ws, events, monotonic_start, deadline)) if send_pings else None
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                return
            events.append(_event("frame_received", monotonic_start, raw=str(raw)))
    finally:
        if ping_task is not None:
            ping_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ping_task


def _close_info_from_exc(exc: BaseException) -> tuple:
    """`ConnectionClosed.code`/`.reason` websockets 13'te deprecated
    (bkz. websockets/exceptions.py) -- once `.rcvd` (kapanma cercevesi)
    denenir, yoksa eski `.code`/`.reason`'a dusulur (12.x uyumu)."""
    rcvd = getattr(exc, "rcvd", None)
    if rcvd is not None:
        return rcvd.code, rcvd.reason
    return getattr(exc, "code", None), getattr(exc, "reason", None)


async def _run(connect_fn=None) -> dict:
    connect_fn = connect_fn or websockets.connect
    events: list = []
    handshake: Optional[dict] = None
    close_code: Optional[int] = None
    close_reason: Optional[str] = None
    error: Optional[str] = None
    connected = False
    monotonic_start = time.monotonic()

    try:
        async with connect_fn(RTDS_WS_URL) as ws:
            connected = True
            handshake = _handshake_info(ws)

            events.append(_event("phase_start", monotonic_start, phase=1, description="abonelik yok"))
            phase1_deadline = time.monotonic() + PHASE1_LISTEN_SECONDS
            await _listen_raw(ws, events, monotonic_start, phase1_deadline, send_pings=False)

            phase2_message = _subscription_message((RTDS_TOPIC_BINANCE,), RTDS_SUBSCRIPTION_TYPE)
            await ws.send(phase2_message)
            events.append(
                _event("subscription_sent", monotonic_start, phase=2, message=phase2_message)
            )
            phase2_deadline = time.monotonic() + PHASE2_LISTEN_SECONDS
            await _listen_raw(ws, events, monotonic_start, phase2_deadline, send_pings=True)

            phase3_message = _subscription_message(PHASE3_CANDIDATE_TOPICS, PHASE3_SUBSCRIPTION_TYPE)
            await ws.send(phase3_message)
            events.append(
                _event("subscription_sent", monotonic_start, phase=3, message=phase3_message)
            )
            phase3_deadline = time.monotonic() + PHASE3_LISTEN_SECONDS
            await _listen_raw(ws, events, monotonic_start, phase3_deadline, send_pings=True)

            with contextlib.suppress(Exception):
                await ws.close()
            close_code = getattr(ws, "close_code", None)
            close_reason = getattr(ws, "close_reason", None)
    except websockets.exceptions.InvalidStatusCode as exc:
        handshake = _handshake_failure_info(exc)
        error = f"{type(exc).__name__}: {exc}"
    except websockets.exceptions.ConnectionClosed as exc:
        connected = True  # baglanti kurulmustu, dinleme sirasinda karsi taraf kapatti
        close_code, close_reason = _close_info_from_exc(exc)
        error = f"{type(exc).__name__}: {exc}"
    except Exception as exc:  # noqa: BLE001 -- prob amacli, her hatayi kaydet, hicbirini yutma
        error = f"{type(exc).__name__}: {exc}"

    frame_events = [e for e in events if e["kind"] == "frame_received"]
    ping_events = [e for e in events if e["kind"] == "ping_sent"]

    return {
        "name": "rtds_raw_capture",
        "url": RTDS_WS_URL,
        "connected": connected,
        "handshake": handshake,
        "phase1_seconds": PHASE1_LISTEN_SECONDS,
        "phase1_description": "abonelik yok",
        "phase2_seconds": PHASE2_LISTEN_SECONDS,
        "phase2_description": f"{RTDS_TOPIC_BINANCE} filtresiz abonelik",
        "phase3_seconds": PHASE3_LISTEN_SECONDS,
        "phase3_description": f"aday crypto-disi topic'ler filtresiz abonelik: {PHASE3_CANDIDATE_TOPICS}",
        "phase3_topics_verified": False,
        "event_count": len(events),
        "frame_received_count": len(frame_events),
        "ping_sent_count": len(ping_events),
        "events": events,
        "close_code": close_code,
        "close_reason": close_reason,
        "error": error,
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def main() -> int:
    run_dir = Path("probe_output") / _now_run_id()
    result = asyncio.run(_run())
    out_path = run_dir / "rtds_raw_capture.json"
    _write_json(out_path, result)
    print(f"rtds_raw_capture tamamlandi: {out_path}")
    print(f"  connected={result['connected']} event_count={result['event_count']}"
          f" frame_received={result['frame_received_count']} ping_sent={result['ping_sent_count']}")
    print(f"  handshake={result['handshake']}")
    print(f"  close_code={result['close_code']} close_reason={result['close_reason']}")
    print(f"  error={result['error']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
