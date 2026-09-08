#!/usr/bin/env python3
"""Tek seferlik prob: gercek Polymarket/borsa uclarina ham cagri yapar,
yanitlari `probe_output/` altina doker.

Bu script:

- HICBIR SEYI DOGRULAMAZ (`validator`den gecirmez, `schemas/` bilmez)
- HICBIR ABONELIK/CACHE KURMAZ -- `collector/` runtime'iyla ilgisi yok
- yalnizca ne donduklerini KAYDEDER

Amac: `collector/gamma_client.py`'nin slug/listing kesif mantigi,
`collector/exchange_probe.py`'nin borsa fallback sirasi ve
`collector/rtds_ws.py`'nin abonelik/sessizlik varsayimlari gibi tasarim
kararlarinin dayandigi varsayimlari gercek uclara karsi tek seferlik
dogrulamak (bkz. docs/decisions.md K-19/K-22/K-23, gamma_client.py modul
docstring'i). Bu sandbox'ta gercek aga erisim engelliydi; bu script'i
calistiran ortamda (yerel makine veya .github/workflows/probe.yml)
erisim var.

Kullanim:
    python -m scripts.probe

Cikti: probe_output/<UTC-ISO-zaman>/*.json + summary.json. Gercek
gozlem verisiyle (`data/`) karistirilmasin diye ayri bir dizin --
repoya commit edilir (bkz. .github/workflows/probe.yml) ama
SCHEMA.md'deki sozlesme kapsamina girmez: dogrulanmamis ham prob
ciktisidir, `data/` altindaki gozlem akisiyla karistirilmamali.
"""

import asyncio
import contextlib
import json
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
import websockets

from collector.endpoints import (
    BINANCE_TICKER_URL,
    CLOB_BOOK_PATH,
    CLOB_REST_BASE_URL,
    COINBASE_TICKER_URL,
    GAMMA_BASE_URL,
    GAMMA_EVENTS_PATH,
    KRAKEN_TICKER_URL,
    RTDS_PING_INTERVAL_SEC,
    RTDS_PING_MESSAGE,
    RTDS_SUBSCRIPTION_TYPE,
    RTDS_SYMBOL_BINANCE,
    RTDS_SYMBOL_CHAINLINK,
    RTDS_TOPIC_BINANCE,
    RTDS_TOPIC_CHAINLINK,
    RTDS_WS_URL,
)
from collector.gamma_client import parse_event_response
from collector.round_calendar import next_round_start_epoch_s, round_slug

RTDS_CAPTURE_SECONDS = 8.0
RTDS_MAX_MESSAGES = 10
RTDS_TYPE_FALLBACK_CAPTURE_SECONDS = 5.0
RTDS_GAP_WINDOW_SECONDS = 60.0
GAMMA_SLUG_PREFIX = "btc-updown-5m-"

_SYMBOL_BY_TOPIC = {
    RTDS_TOPIC_BINANCE: RTDS_SYMBOL_BINANCE,
    RTDS_TOPIC_CHAINLINK: RTDS_SYMBOL_CHAINLINK,
}


def _now_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


async def _probe_http(
    client: httpx.AsyncClient, name: str, method: str, url: str, *, params: Optional[dict] = None
) -> dict:
    started = time.monotonic()
    result = {
        "name": name,
        "method": method,
        "url": url,
        "params": params,
        "requested_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    try:
        response = await client.request(method, url, params=params)
        result["status_code"] = response.status_code
        result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
        result["headers"] = dict(response.headers)
        result["body_text"] = response.text[:20000]
        try:
            result["body_json"] = response.json()
        except ValueError:
            result["body_json"] = None
        result["error"] = None
    except Exception as exc:  # noqa: BLE001 -- prob amacli, her hatayi kaydet, hicbirini yutma
        result["status_code"] = None
        result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
        result["headers"] = None
        result["body_text"] = None
        result["body_json"] = None
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _subscription_message(topics_and_types: list) -> str:
    """`[(topic, type), ...]` -> RTDS abonelik zarfi. `filters` HER ZAMAN
    bir JSON string'dir (nesne degil) ve sembol topic'e gore degisir --
    bkz. collector/rtds_ws.py modul docstring'i, docs/decisions.md."""
    return json.dumps(
        {
            "subscriptions": [
                {
                    "topic": topic,
                    "type": sub_type,
                    "filters": json.dumps({"symbol": _SYMBOL_BY_TOPIC[topic]}),
                }
                for topic, sub_type in topics_and_types
            ]
        }
    )


async def _ping_loop(ws, stop_event: asyncio.Event) -> None:
    """Uretim istemcisiyle ayni kadans: 5 saniyede bir "PING" text frame'i
    (bkz. collector/endpoints.py RTDS_PING_INTERVAL_SEC/RTDS_PING_MESSAGE).
    Prob kisa sureli oldugu icin sunucunun ping bekleyip beklemedigi bu
    olmadan test edilemezdi."""
    while not stop_event.is_set():
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop_event.wait(), timeout=RTDS_PING_INTERVAL_SEC)
            return
        with contextlib.suppress(Exception):
            await ws.send(RTDS_PING_MESSAGE)


async def _probe_rtds(connect_fn=None) -> dict:
    """Ilk mesajlari ve alan adlarini gormek icin kisa sureli dinleme.
    `type="update"` ile denenir; chainlink'ten hic mesaj gelmezse
    `type="*"` ile ayri bir kisa deneme daha yapilir (bkz.
    docs/decisions.md, "type icin update ile basla, gelmezse * dene")."""
    connect_fn = connect_fn or websockets.connect
    messages = []
    error = None
    connected = False
    try:
        async with connect_fn(RTDS_WS_URL) as ws:
            connected = True
            await ws.send(
                _subscription_message(
                    [
                        (RTDS_TOPIC_BINANCE, RTDS_SUBSCRIPTION_TYPE),
                        (RTDS_TOPIC_CHAINLINK, RTDS_SUBSCRIPTION_TYPE),
                    ]
                )
            )

            stop_event = asyncio.Event()
            ping_task = asyncio.create_task(_ping_loop(ws, stop_event))
            try:
                deadline = time.monotonic() + RTDS_CAPTURE_SECONDS
                while time.monotonic() < deadline and len(messages) < RTDS_MAX_MESSAGES:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                    except asyncio.TimeoutError:
                        break
                    try:
                        messages.append(json.loads(raw))
                    except (json.JSONDecodeError, TypeError):
                        messages.append({"unparsed_raw": str(raw)[:2000]})
            finally:
                stop_event.set()
                ping_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await ping_task
    except Exception as exc:  # noqa: BLE001 -- prob amacli, her hatayi kaydet
        error = f"{type(exc).__name__}: {exc}"

    chainlink_seen = any(m.get("topic") == RTDS_TOPIC_CHAINLINK for m in messages if isinstance(m, dict))

    result = {
        "name": "rtds_first_messages",
        "url": RTDS_WS_URL,
        "subscription_type_tried": RTDS_SUBSCRIPTION_TYPE,
        "connected": connected,
        "message_count": len(messages),
        "messages": messages,
        "error": error,
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    if connected and not chainlink_seen:
        result["type_star_fallback"] = await _probe_rtds_type_fallback(connect_fn, "*")

    return result


async def _probe_rtds_type_fallback(connect_fn, fallback_type: str) -> dict:
    """`type="update"` ile chainlink'ten hic mesaj gelmediginde denenen
    kisa alternatif abonelik. Yalnizca chainlink icin -- binance zaten
    mesaj verdiyse "update" dogrulanmis sayilir."""
    messages = []
    error = None
    connected = False
    try:
        async with connect_fn(RTDS_WS_URL) as ws:
            connected = True
            await ws.send(_subscription_message([(RTDS_TOPIC_CHAINLINK, fallback_type)]))
            deadline = time.monotonic() + RTDS_TYPE_FALLBACK_CAPTURE_SECONDS
            while time.monotonic() < deadline:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                try:
                    messages.append(json.loads(raw))
                except (json.JSONDecodeError, TypeError):
                    messages.append({"unparsed_raw": str(raw)[:2000]})
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"

    return {
        "type_tried": fallback_type,
        "connected": connected,
        "message_count": len(messages),
        "messages": messages,
        "error": error,
    }


def _gap_stats(arrivals_monotonic: list) -> dict:
    """Ardisik varis zamanlari arasindaki farklardan min/medyan/maks/sayi.
    `count` mesaj sayisi (gap sayisi degil) -- eslik eden N-1 gap'ten
    dagilim cikarilir; <2 mesajda gap yok, `gaps_sec` None."""
    count = len(arrivals_monotonic)
    if count < 2:
        return {"count": count, "gaps_sec": None}
    gaps = [b - a for a, b in zip(arrivals_monotonic, arrivals_monotonic[1:])]
    return {
        "count": count,
        "gaps_sec": {
            "min": min(gaps),
            "median": statistics.median(gaps),
            "max": max(gaps),
        },
    }


async def _probe_rtds_gap_distribution(connect_fn=None) -> dict:
    """K-23: sessizlik esiklerini (silence_warn_sec/silence_reconnect_sec)
    tahminle degil olcumle sabitlemek icin -- RTDS'i RTDS_GAP_WINDOW_SECONDS
    (60s) dinler, topic basina mesajlar-arasi gecikme dagilimini
    (min/medyan/maks/sayi) raporlar."""
    connect_fn = connect_fn or websockets.connect
    arrivals: dict = {RTDS_TOPIC_BINANCE: [], RTDS_TOPIC_CHAINLINK: []}
    error = None
    connected = False
    try:
        async with connect_fn(RTDS_WS_URL) as ws:
            connected = True
            await ws.send(
                _subscription_message(
                    [
                        (RTDS_TOPIC_BINANCE, RTDS_SUBSCRIPTION_TYPE),
                        (RTDS_TOPIC_CHAINLINK, RTDS_SUBSCRIPTION_TYPE),
                    ]
                )
            )

            stop_event = asyncio.Event()
            ping_task = asyncio.create_task(_ping_loop(ws, stop_event))
            try:
                deadline = time.monotonic() + RTDS_GAP_WINDOW_SECONDS
                while time.monotonic() < deadline:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                    except asyncio.TimeoutError:
                        break
                    try:
                        envelope = json.loads(raw)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    topic = envelope.get("topic")
                    payload = envelope.get("payload")
                    if topic in arrivals and isinstance(payload, dict) and "value" in payload:
                        arrivals[topic].append(time.monotonic())
            finally:
                stop_event.set()
                ping_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await ping_task
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"

    return {
        "name": "rtds_gap_distribution",
        "window_sec": RTDS_GAP_WINDOW_SECONDS,
        "connected": connected,
        "error": error,
        "per_topic": {topic: _gap_stats(times) for topic, times in arrivals.items()},
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def _extract_first_token_id(event: dict) -> Optional[str]:
    """Ham event'ten token_id cikarmayi dener; kesif deseni yanlissa
    (alan adlari beklenenden farkli) None doner, script cokmez."""
    try:
        market = parse_event_response([event], 0)
    except Exception:
        return None
    if market is None:
        return None
    return market.token_ids.get("up")


async def _run() -> dict:
    run_dir = Path("probe_output") / _now_run_id()
    results = []
    token_id_for_book = None

    target_epoch_s = next_round_start_epoch_s(time.time())
    target_slug = round_slug(target_epoch_s)

    async with httpx.AsyncClient(timeout=15.0) as client:
        # 1) Gamma slug denemesi -- gamma_client.fetch_round_market'in hizli yolu
        slug_result = await _probe_http(
            client,
            "gamma_slug",
            "GET",
            f"{GAMMA_BASE_URL}{GAMMA_EVENTS_PATH}",
            params={"slug": target_slug},
        )
        results.append(slug_result)
        _write_json(run_dir / "gamma_slug.json", slug_result)

        events = slug_result.get("body_json")
        if isinstance(events, list) and events:
            token_id_for_book = _extract_first_token_id(events[0])

        # 2) Gamma listeleme ucu -- MEVCUT (bilinen-bozuk) parametreler.
        # gamma_client.fetch_round_market_via_listing UNVERIFIED olarak
        # isaretli (bkz. o dosyanin docstring'i) -- bu deger degistirilmedi,
        # yalnizca asagida ADAY alternatifler ayrica problaniyor.
        listing_result = await _probe_http(
            client,
            "gamma_listing",
            "GET",
            f"{GAMMA_BASE_URL}{GAMMA_EVENTS_PATH}",
            params={
                "active": "true",
                "closed": "false",
                "order": "startDate",
                "ascending": "true",
                "limit": 100,
                "offset": 0,
            },
        )
        results.append(listing_result)
        _write_json(run_dir / "gamma_listing.json", listing_result)

        if token_id_for_book is None:
            listing_events = listing_result.get("body_json")
            if isinstance(listing_events, list):
                for event in listing_events:
                    slug = event.get("slug") or ""
                    if slug.startswith(GAMMA_SLUG_PREFIX):
                        token_id_for_book = _extract_first_token_id(event)
                        if token_id_for_book:
                            break

        # 2b) ADAY yedek-yol parametreleri -- hicbiri collector'a
        # otomatik uygulanmiyor (CLAUDE.md: tahmin etme). Amac: hangisi
        # guncel/yakin donemdeki 5dk round'u ilk sayfaya getiriyor,
        # gercek yanitla gorup gamma_client.py'yi SONRA (bu prob ciktisi
        # okunduktan sonra) guncellemek.
        now_iso = datetime.now(timezone.utc).isoformat()
        candidate_params = {
            "gamma_listing_order_enddate_asc": {
                "active": "true",
                "closed": "false",
                "order": "endDate",
                "ascending": "true",
                "limit": 100,
                "offset": 0,
            },
            "gamma_listing_order_enddate_desc": {
                "active": "true",
                "closed": "false",
                "order": "endDate",
                "ascending": "false",
                "limit": 100,
                "offset": 0,
            },
            "gamma_listing_end_date_min": {
                "active": "true",
                "closed": "false",
                "end_date_min": now_iso,
                "limit": 100,
                "offset": 0,
            },
            "gamma_listing_order_id_desc": {
                "active": "true",
                "closed": "false",
                "order": "id",
                "ascending": "false",
                "limit": 100,
                "offset": 0,
            },
        }
        for probe_name, params in candidate_params.items():
            candidate_result = await _probe_http(
                client,
                probe_name,
                "GET",
                f"{GAMMA_BASE_URL}{GAMMA_EVENTS_PATH}",
                params=params,
            )
            results.append(candidate_result)
            _write_json(run_dir / f"{probe_name}.json", candidate_result)

        # 3) CLOB book -- yalnizca yukarida bir token_id bulunabildiyse
        if token_id_for_book:
            book_result = await _probe_http(
                client,
                "clob_book",
                "GET",
                f"{CLOB_REST_BASE_URL}{CLOB_BOOK_PATH}",
                params={"token_id": token_id_for_book},
            )
        else:
            book_result = {
                "name": "clob_book",
                "skipped": True,
                "reason": "gamma'dan token_id bulunamadi (slug ve listing ikisi de basarisiz/eslesmedi)",
            }
        results.append(book_result)
        _write_json(run_dir / "clob_book.json", book_result)

        # 4) uc borsa -- hepsi denenir, ilkinde durulmaz (amac karsilastirma,
        # K-19). Sira, collector/exchange_probe.py'deki gercek fallback
        # sirasiyla (Coinbase -> Kraken -> Binance) tutarli.
        for name, url in (
            ("exchange_coinbase", COINBASE_TICKER_URL),
            ("exchange_kraken", KRAKEN_TICKER_URL),
            ("exchange_binance", BINANCE_TICKER_URL),
        ):
            exch_result = await _probe_http(client, name, "GET", url)
            results.append(exch_result)
            _write_json(run_dir / f"{name}.json", exch_result)

    # 5) RTDS ilk mesajlar -- ayri, WS (kisa sureli tek seferlik dinleme,
    # gerekirse type="*" fallback denemesi dahil)
    rtds_result = await _probe_rtds()
    results.append(rtds_result)
    _write_json(run_dir / "rtds_messages.json", rtds_result)

    # 6) RTDS 60s gecikme dagilimi -- K-23: sessizlik esiklerini olcumle
    # sabitlemek icin
    rtds_gap_result = await _probe_rtds_gap_distribution()
    results.append(rtds_gap_result)
    _write_json(run_dir / "rtds_gap_distribution.json", rtds_gap_result)

    summary = {
        "run_dir": str(run_dir),
        "target_slug": target_slug,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "probes": [
            {
                "name": r.get("name"),
                "skipped": r.get("skipped", False),
                "ok": (
                    False
                    if r.get("skipped")
                    else r.get("connected", r.get("status_code") == 200) and not r.get("error")
                ),
                "status_code": r.get("status_code"),
                "error": r.get("error"),
            }
            for r in results
        ],
    }
    _write_json(run_dir / "summary.json", summary)
    return summary


def main() -> int:
    summary = asyncio.run(_run())
    print(f"Prob tamamlandi: {summary['run_dir']}")
    for probe in summary["probes"]:
        state = "SKIP" if probe["skipped"] else ("OK" if probe["ok"] else "FAIL")
        print(f"  [{state}] {probe['name']} status={probe['status_code']} error={probe['error']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
