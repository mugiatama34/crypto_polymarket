#!/usr/bin/env python3
"""Tek seferlik prob: gercek Polymarket/borsa uclarina ham cagri yapar,
yanitlari `probe_output/` altina doker.

Bu script:

- HICBIR SEYI DOGRULAMAZ (`validator`den gecirmez, `schemas/` bilmez)
- HICBIR ABONELIK/CACHE KURMAZ -- `collector/` runtime'iyla ilgisi yok
- yalnizca ne donduklerini KAYDEDER

Amac: `collector/gamma_client.py`'nin slug/listing kesif mantigi ve
`collector/exchange_probe.py`'nin borsa fallback sirasi gibi tasarim
kararlarinin dayandigi varsayimlari gercek uclara karsi tek seferlik
dogrulamak (bkz. docs/decisions.md K-19, gamma_client.py modul
docstring'i -- "birincil kaynaktan tam teyit edilemedi" notlari). Bu
sandbox'ta gercek aga erisim engelliydi; bu script'i calistiran ortamda
(yerel makine veya .github/workflows/probe.yml) erisim var.

Kullanim:
    python -m scripts.probe

Cikti: probe_output/<UTC-ISO-zaman>/*.json + summary.json. Gercek
gozlem verisiyle (`data/`) karistirilmasin diye ayri bir dizin --
git'e commit edilmez (bkz. .gitignore).
"""

import asyncio
import json
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
    RTDS_BTC_SYMBOL,
    RTDS_SUBSCRIPTION_TYPE,
    RTDS_TOPIC_BINANCE,
    RTDS_TOPIC_CHAINLINK,
    RTDS_WS_URL,
)
from collector.gamma_client import parse_event_response
from collector.round_calendar import next_round_start_epoch_s, round_slug

RTDS_CAPTURE_SECONDS = 8.0
RTDS_MAX_MESSAGES = 10
GAMMA_SLUG_PREFIX = "btc-updown-5m-"


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


async def _probe_rtds(connect_fn=None) -> dict:
    connect_fn = connect_fn or websockets.connect
    messages = []
    error = None
    connected = False
    try:
        async with connect_fn(RTDS_WS_URL) as ws:
            connected = True
            subscription = {
                "subscriptions": [
                    {
                        "topic": topic,
                        "type": RTDS_SUBSCRIPTION_TYPE,
                        "filters": json.dumps({"symbol": RTDS_BTC_SYMBOL}),
                    }
                    for topic in (RTDS_TOPIC_BINANCE, RTDS_TOPIC_CHAINLINK)
                ]
            }
            await ws.send(json.dumps(subscription))

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
    except Exception as exc:  # noqa: BLE001 -- prob amacli, her hatayi kaydet
        error = f"{type(exc).__name__}: {exc}"

    return {
        "name": "rtds_first_messages",
        "url": RTDS_WS_URL,
        "connected": connected,
        "message_count": len(messages),
        "messages": messages,
        "error": error,
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

        # 2) Gamma listeleme ucu -- gamma_client.fetch_round_market_via_listing'in yedek yolu
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

        # 5) uc borsa -- hepsi denenir, ilkinde durulmaz (amac karsilastirma, K-19)
        for name, url in (
            ("exchange_binance", BINANCE_TICKER_URL),
            ("exchange_coinbase", COINBASE_TICKER_URL),
            ("exchange_kraken", KRAKEN_TICKER_URL),
        ):
            exch_result = await _probe_http(client, name, "GET", url)
            results.append(exch_result)
            _write_json(run_dir / f"{name}.json", exch_result)

    # 4) RTDS ilk mesajlar -- ayri, WS (kisa sureli tek seferlik dinleme)
    rtds_result = await _probe_rtds()
    results.append(rtds_result)
    _write_json(run_dir / "rtds_messages.json", rtds_result)

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
