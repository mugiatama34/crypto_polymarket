#!/usr/bin/env python3
"""Ikinci tur izolasyon probu: `scripts/rtds_raw_capture.py`'nin ortaya
cikardigi sessizligi (bkz. probe_output/20260909T063155Z --
frame_received_count: 0, close_code: 1000, handshake'te __cf_bm +
CF-RAY) daha fazla ayirmak icin. O prob "crypto'ya ozgu mu" sorusunu
cevapladi (hayir -- abonelik mekanizmasinin tamami sessiz, bkz.
docs/decisions.md K-25). Bu prob uc YENI eksen olcer, HICBIRINI
DUZELTMEZ:

  A) Abonelik zarfinin SEKLI sessizligi degistiriyor mu? Dort varyant,
     dort AYRI baglanti (biri digerini bulandirmasin diye), hepsi
     `crypto_prices` topic'ine, PRODUKSIYON filters alaniyla (bkz.
     collector/rtds_ws.py._handle_open) -- yalnizca sarmalayici/alan adi
     degisir, her varyant TEK bir degiskeni izole eder:
       A1: {"action": "subscribe", "subscriptions": [...]}
       A2: mevcut/produksiyon sekli -- sarmalayici yok, action yok
       A3: tek abonelik nesnesi, "subscriptions" sarmalayicisi olmadan
       A4: tek abonelik nesnesi, "subscription" (tekil) alani icinde

  B) Origin/User-Agent header'lari sessizligi degistiriyor mu?
     Cloudflare bot-yonetimi hipotezi dogruysa tarayicidan gelmis gibi
     gorunen bir el sikisma farkli davranabilir. Iki baglanti: biriyle
     Origin=https://polymarket.com + temsili bir tarayici User-Agent'i,
     biriyle (ayni koyumda, karsilastirma icin) hic ozel header yok.

  C) Sunucu 60 saniye BEKLEMEDEN (abonelik gonderilmeden) kendiliginden
     bir sey yolluyor mu? `rtds_raw_capture.py`'nin faz 1'i bunu 15s
     dinliyordu -- burada 60s'e cikariliyor. Hem uygulama seviyesi
     cerceveler (`.recv()`) hem GERCEK WS protokol kontrol cerceveleri
     (PING/PONG/CLOSE -- bunlar `.recv()`'e hic dusmez, `websockets`
     kutuphanesi seffaf yutar) ayri olarak kaydedilir (bkz.
     `_capture_protocol_frames`).

  D) Baglanti omru: 20-30 saniyelik pencereler yeterince uzun mu?
     Chainlink TWAP feed'i sakin piyasada seyrek yayinliyorsa kisa
     pencereler "sessizlik" gibi gorunup aslinda "henuz siranin gelmedigi"
     olabilir. Produksiyon sekliyle abone olunup 5 DAKIKA (ping'lerle,
     produksiyon kadansiyla -- RTDS_PING_INTERVAL_SEC) dinlenir.

Her eksen/varyant AYRI bir JSON dosyasina yazilir; hicbiri digeriyle
birlestirilmez. Her sonuçta Cloudflare isaretleri (`__cf_bm` cerezinin
varligi, `CF-RAY` degeri) header listesinin icinde gomulu kalmaz --
`cf_signals` adinda ayri, duz bir alan olarak da cikarilir (karsilastirma
icin tek bakista okunsun).

Bu prob `scripts/probe.py` ve `scripts/rtds_raw_capture.py` ile aynı
ilkeye baglidir: HICBIR SEYI DOGRULAMAZ (validator/schemas'tan gecmez),
collector/ ile ilgisi yoktur, uretim koduna baglanmaz. User-Agent
string'i ADAY/TEMSILIDIR (asagida isaretli) -- gercek bir tarayicidan
yakalanmadi.

Kullanim:
    python -m scripts.rtds_cf_diagnosis

Cikti: probe_output/<UTC-ISO-zaman>/*.json + summary.json
"""

import asyncio
import contextlib
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import websockets
import websockets.exceptions

from collector.endpoints import (
    RTDS_PING_INTERVAL_SEC,
    RTDS_SUBSCRIPTION_TYPE,
    RTDS_SYMBOL_BINANCE,
    RTDS_TOPIC_BINANCE,
    RTDS_WS_URL,
)
from scripts.probe import _write_json
from scripts.rtds_raw_capture import (
    _close_info_from_exc,
    _event,
    _handshake_failure_info,
    _handshake_info,
    _listen_raw,
    _now_run_id,
)

SHAPE_LISTEN_SECONDS = 20.0
HEADER_LISTEN_SECONDS = 30.0
PASSIVE_LISTEN_SECONDS = 60.0
LIFETIME_LISTEN_SECONDS = 300.0

# ADAY/TEMSILI: gercek bir tarayicidan yakalanmis degil, tipik bir
# Chrome-on-Windows User-Agent formati. Cloudflare hipotezini test etmek
# icin "tarayicidan gelmis gibi gorunen" bir deger gerekiyordu -- bu
# string'in kendisi degil, "Python-websockets degil, bir tarayici UA'si
# gonderildi" olgusu onemli.
REPRESENTATIVE_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)
ORIGIN_HEADER_VALUE = "https://polymarket.com"


def _crypto_prices_filters_json() -> str:
    """`filters` PRODUKSIYONDA da bir JSON STRING'dir (nesne degil) --
    bkz. collector/rtds_ws.py modul docstring'i. Bu eksen SADECE zarf
    sekli degiskenini izole eder, filters alanini da kaldirirsa iki
    degisken birden degismis olur."""
    return json.dumps({"symbol": RTDS_SYMBOL_BINANCE})


def _production_subscription() -> dict:
    """collector/rtds_ws.py._handle_open'daki GERCEK uretim zarfi,
    yalnizca `crypto_prices_chainlink` girdisi cikarilmis (bu prob tek
    topic'e odaklaniyor)."""
    return {
        "topic": RTDS_TOPIC_BINANCE,
        "type": RTDS_SUBSCRIPTION_TYPE,
        "filters": _crypto_prices_filters_json(),
    }


_SINGLE_SUBSCRIPTION = _production_subscription()

SHAPE_A1_ACTION_FIELD = json.dumps(
    {"action": "subscribe", "subscriptions": [_SINGLE_SUBSCRIPTION]}
)
SHAPE_A2_CURRENT = json.dumps({"subscriptions": [_SINGLE_SUBSCRIPTION]})
SHAPE_A3_SINGLE_OBJECT = json.dumps(_SINGLE_SUBSCRIPTION)
SHAPE_A4_SINGULAR_KEY = json.dumps({"subscription": _SINGLE_SUBSCRIPTION})


class _FrameLogHandler(logging.Handler):
    """`websockets.legacy.protocol`'un DEBUG seviyesinde yazdigi
    `"< %s", frame` / `"> %s", frame` log satirlarini yakalar. Bu,
    `.recv()`'in GORMEDIGI gercek WS protokol kontrol cercevelerini
    (PING/PONG/CLOSE) de ortaya cikarir -- kutuphane bunlari sessizce
    yanitlayip uygulamaya hic iletmez (bkz. websockets/legacy/protocol.py
    `read_data_frame`). Sadece "< " / "> " ile baslayan kayitlar alinir;
    bu logger'in baska DEBUG satirlari (baglanti durumu vb.) atlanir."""

    def __init__(self, monotonic_start: float):
        super().__init__(level=logging.DEBUG)
        self._monotonic_start = monotonic_start
        self.events: list = []

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.getMessage()
        if msg.startswith("< "):
            kind = "protocol_frame_received"
        elif msg.startswith("> "):
            kind = "protocol_frame_sent"
        else:
            return
        self.events.append(
            _event(kind, self._monotonic_start, frame_repr=msg[2:])
        )


@contextlib.contextmanager
def _capture_protocol_frames(monotonic_start: float):
    logger = logging.getLogger("websockets.protocol")
    prev_level = logger.level
    handler = _FrameLogHandler(monotonic_start)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    try:
        yield handler
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prev_level)


def _extract_cf_signals(handshake: Optional[dict]) -> dict:
    """Cloudflare bot-yonetimi isaretlerini (`__cf_bm` cerezi, `CF-RAY`)
    header listesinden ayri, duz alanlara cikarir -- hipotezi
    karsilastirirken header listesinin icinde aramak gerekmesin."""
    headers = (handshake or {}).get("response_headers")
    cf_bm_present = False
    cf_ray = None
    if headers:
        for name, value in headers:
            lname = name.lower()
            if lname == "set-cookie" and "__cf_bm=" in value:
                cf_bm_present = True
            elif lname == "cf-ray":
                cf_ray = value
    return {"cf_bm_cookie_present": cf_bm_present, "cf_ray": cf_ray}


async def _run_connection(
    name: str,
    *,
    subscribe_message: Optional[str],
    listen_seconds: float,
    send_app_pings: bool,
    origin: Optional[str] = None,
    user_agent_header: Optional[str] = None,
    connect_fn=None,
) -> dict:
    """Tek senaryo, TEK baglanti: bagla, (varsa) abonelik gonder,
    `listen_seconds` dinle, kapat. Uygulama seviyesi cerceveler
    (`.recv()`, `kind=frame_received`) ve protokol seviyesi cerceveler
    (`_capture_protocol_frames`, `kind=protocol_frame_*`) ayni `events`
    listesinde ama `kind` alaniyla ayirt edilebilir sekilde durur."""
    connect_fn = connect_fn or websockets.connect
    connect_kwargs: dict = {}
    if origin is not None:
        connect_kwargs["origin"] = origin
    if user_agent_header is not None:
        connect_kwargs["user_agent_header"] = user_agent_header

    events: list = []
    handshake: Optional[dict] = None
    close_code: Optional[int] = None
    close_reason: Optional[str] = None
    error: Optional[str] = None
    connected = False
    monotonic_start = time.monotonic()

    with _capture_protocol_frames(monotonic_start) as frame_capture:
        try:
            async with connect_fn(RTDS_WS_URL, **connect_kwargs) as ws:
                connected = True
                handshake = _handshake_info(ws)

                if subscribe_message is not None:
                    await ws.send(subscribe_message)
                    events.append(
                        _event("subscription_sent", monotonic_start, message=subscribe_message)
                    )

                deadline = time.monotonic() + listen_seconds
                await _listen_raw(ws, events, monotonic_start, deadline, send_pings=send_app_pings)

                with contextlib.suppress(Exception):
                    await ws.close()
                close_code = getattr(ws, "close_code", None)
                close_reason = getattr(ws, "close_reason", None)
        except websockets.exceptions.InvalidStatusCode as exc:
            handshake = _handshake_failure_info(exc)
            error = f"{type(exc).__name__}: {exc}"
        except websockets.exceptions.ConnectionClosed as exc:
            connected = True
            close_code, close_reason = _close_info_from_exc(exc)
            error = f"{type(exc).__name__}: {exc}"
        except Exception as exc:  # noqa: BLE001 -- prob amacli, her hatayi kaydet
            error = f"{type(exc).__name__}: {exc}"
        finally:
            events.extend(frame_capture.events)

    events.sort(key=lambda e: e["elapsed_sec"])

    frame_events = [e for e in events if e["kind"] == "frame_received"]
    protocol_received = [e for e in events if e["kind"] == "protocol_frame_received"]
    protocol_sent = [e for e in events if e["kind"] == "protocol_frame_sent"]

    return {
        "name": name,
        "url": RTDS_WS_URL,
        "connected": connected,
        "handshake": handshake,
        "cf_signals": _extract_cf_signals(handshake),
        "origin_header_sent": origin,
        "user_agent_header_sent": user_agent_header,
        "subscribe_message_sent": subscribe_message,
        "listen_seconds": listen_seconds,
        "app_pings_sent": send_app_pings,
        "event_count": len(events),
        "frame_received_count": len(frame_events),
        "protocol_frame_received_count": len(protocol_received),
        "protocol_frame_sent_count": len(protocol_sent),
        "events": events,
        "close_code": close_code,
        "close_reason": close_reason,
        "error": error,
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    }


async def _run(connect_fn=None) -> dict:
    run_dir = Path("probe_output") / _now_run_id()
    summary_rows = []

    # Eksen A -- abonelik zarfi sekli, 4 AYRI baglanti.
    shape_variants = (
        ("shape_a1_action_field", SHAPE_A1_ACTION_FIELD),
        ("shape_a2_current", SHAPE_A2_CURRENT),
        ("shape_a3_single_object", SHAPE_A3_SINGLE_OBJECT),
        ("shape_a4_singular_key", SHAPE_A4_SINGULAR_KEY),
    )
    for probe_name, message in shape_variants:
        result = await _run_connection(
            probe_name,
            subscribe_message=message,
            listen_seconds=SHAPE_LISTEN_SECONDS,
            send_app_pings=False,
            connect_fn=connect_fn,
        )
        _write_json(run_dir / f"{probe_name}.json", result)
        summary_rows.append(result)

    # Eksen B -- Origin/User-Agent header testi, 2 baglanti, mevcut
    # (A2) zarfiyla -- tek degisken header'lar olsun diye zarf sabit.
    header_variants = (
        (
            "headers_with_origin_ua",
            {"origin": ORIGIN_HEADER_VALUE, "user_agent_header": REPRESENTATIVE_BROWSER_USER_AGENT},
        ),
        ("headers_none", {}),
    )
    for probe_name, kwargs in header_variants:
        result = await _run_connection(
            probe_name,
            subscribe_message=SHAPE_A2_CURRENT,
            listen_seconds=HEADER_LISTEN_SECONDS,
            send_app_pings=True,
            connect_fn=connect_fn,
            **kwargs,
        )
        _write_json(run_dir / f"{probe_name}.json", result)
        summary_rows.append(result)

    # Eksen C -- 60s pasif dinleme, abonelik YOK.
    passive_result = await _run_connection(
        "passive_60s",
        subscribe_message=None,
        listen_seconds=PASSIVE_LISTEN_SECONDS,
        send_app_pings=False,
        connect_fn=connect_fn,
    )
    _write_json(run_dir / "passive_60s.json", passive_result)
    summary_rows.append(passive_result)

    # Eksen D -- baglanti omru, (A2) zarfiyla 5dk, produksiyon ping
    # kadansiyla (RTDS_PING_INTERVAL_SEC).
    lifetime_result = await _run_connection(
        "lifetime_5min",
        subscribe_message=SHAPE_A2_CURRENT,
        listen_seconds=LIFETIME_LISTEN_SECONDS,
        send_app_pings=True,
        connect_fn=connect_fn,
    )
    _write_json(run_dir / "lifetime_5min.json", lifetime_result)
    summary_rows.append(lifetime_result)

    summary = {
        "run_dir": str(run_dir),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "ping_interval_sec_used_for_app_pings": RTDS_PING_INTERVAL_SEC,
        "probes": [
            {
                "name": r["name"],
                "connected": r["connected"],
                "frame_received_count": r["frame_received_count"],
                "protocol_frame_received_count": r["protocol_frame_received_count"],
                "cf_signals": r["cf_signals"],
                "close_code": r["close_code"],
                "close_reason": r["close_reason"],
                "error": r["error"],
            }
            for r in summary_rows
        ],
    }
    _write_json(run_dir / "summary.json", summary)
    return summary


def main() -> int:
    summary = asyncio.run(_run())
    print(f"rtds_cf_diagnosis tamamlandi: {summary['run_dir']}")
    for probe in summary["probes"]:
        print(
            f"  [{probe['name']}] connected={probe['connected']}"
            f" frame_received={probe['frame_received_count']}"
            f" protocol_frame_received={probe['protocol_frame_received_count']}"
            f" cf_signals={probe['cf_signals']}"
            f" close=({probe['close_code']!r}, {probe['close_reason']!r})"
            f" error={probe['error']}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
