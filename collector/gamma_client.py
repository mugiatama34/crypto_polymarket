"""Gamma API'den market kesfi: iki yol.

1. **slug** (hizli yol): round zamanlamasi 300s'e hizali ve onceden
   bilindigi icin dogrudan GET {GAMMA_BASE_URL}/events?slug=btc-updown-5m-<epoch>
   sorgulanir. Slug deseni (SCHEMA.md round_id ornegiyle tutarli) birincil
   Polymarket kaynagindan tam teyit edilemedi.
2. **listing** (yedek yol): slug bulunamazsa -- desen yanlissa veya
   market henuz/artik o slug'la listelenmiyorsa -- Gamma'nin dogrulanmis
   `active`/`closed`/`order`/`ascending`/`limit`/`offset` parametreleriyle
   sayfalanarak listelenir, `btc-updown-5m-` on ekiyle baslayan event'ler
   arasindan close_ts'i bizim hesapladigimiz hedefe (round_start_s+300s)
   esit olan secilir. Bu yol slug deseni tamamen yanlis olsa bile
   calisir, ama daha yavastir (birden fazla sayfa cekebilir).

Hangi yolun kullanildigi `RoundMarket.discovery_method` alaninda durur
("slug" | "listing"); cagiran taraf (runner.py) bunu round kaydinin
`raw[]` girdisine etiket olarak yazar.

Her iki yolda da bulunamazsa None doner; cagiran taraf (sampler/runner)
round'u `status: "missed"` ile yazar, sessizce dusurulmez.

open_ts/close_ts icin Gamma yanitinda taninan bir tarih alani varsa o
kullanilir; yoksa slug'in kendisinin kodladigi baslangic epoch'una (ve
+300s'e) dusulur -- slug'i Polymarket'in kendisi uretiyor, bu yuzden
"market'in bildirdigi" tanimina aykiri sayilmiyor. Ham event yaniti
`raw` alaninda degismeden saklanir.
"""

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import httpx

from .endpoints import GAMMA_BASE_URL, GAMMA_EVENTS_PATH
from .round_calendar import ROUND_SECONDS, round_slug

_START_DATE_FIELDS = ("startDate", "startTime", "gameStartTime")
_END_DATE_FIELDS = ("endDate", "endTime", "gameEndTime")
_SLUG_PREFIX = "btc-updown-5m-"
_LISTING_PAGE_SIZE = 100
_LISTING_MAX_PAGES = 5


class MarketParseError(Exception):
    def __init__(self, message: str, raw: dict):
        super().__init__(message)
        self.raw = raw


@dataclass
class RoundMarket:
    round_id: str
    condition_id: str
    token_ids: dict
    open_ts_ms: int
    close_ts_ms: int
    raw: dict
    discovery_method: str = "slug"


def _parse_json_array_field(market: dict, field_name: str) -> list:
    value = market.get(field_name)
    if isinstance(value, str):
        return json.loads(value)
    if isinstance(value, list):
        return value
    raise MarketParseError(f"'{field_name}' alani bulunamadi veya beklenmeyen tipte", market)


def _extract_token_ids(market: dict) -> dict:
    outcomes = _parse_json_array_field(market, "outcomes")
    token_ids = _parse_json_array_field(market, "clobTokenIds")
    if len(outcomes) != len(token_ids):
        raise MarketParseError("outcomes/clobTokenIds uzunluklari uyusmuyor", market)

    mapping = {}
    for outcome, token_id in zip(outcomes, token_ids):
        label = str(outcome).strip().lower()
        if label in ("up", "down"):
            mapping[label] = str(token_id)
    if "up" not in mapping or "down" not in mapping:
        raise MarketParseError(f"outcomes icinde up/down eslesmedi: {outcomes!r}", market)
    return mapping


def _parse_iso_ms(value: str) -> Optional[int]:
    try:
        cleaned = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except (ValueError, AttributeError):
        return None


def _extract_ts_ms(market: dict, fields: tuple, fallback_ms: int) -> int:
    for field_name in fields:
        raw_value = market.get(field_name)
        if isinstance(raw_value, str):
            parsed = _parse_iso_ms(raw_value)
            if parsed is not None:
                return parsed
        elif isinstance(raw_value, (int, float)):
            return int(raw_value)
    return fallback_ms


def _select_market_object(event: dict) -> dict:
    markets = event.get("markets")
    if isinstance(markets, list) and markets:
        merged = dict(event)
        merged.update(markets[0])
        return merged
    return event


def _epoch_from_slug(slug: str, prefix: str = _SLUG_PREFIX) -> Optional[int]:
    if not slug or not slug.startswith(prefix):
        return None
    try:
        return int(slug[len(prefix):])
    except ValueError:
        return None


def _build_round_market(event: dict, start_epoch_s: int, *, discovery_method: str) -> RoundMarket:
    market = _select_market_object(event)

    condition_id = market.get("conditionId")
    if not condition_id:
        raise MarketParseError("conditionId alani bulunamadi", market)

    token_ids = _extract_token_ids(market)

    fallback_open_ms = start_epoch_s * 1000
    fallback_close_ms = (start_epoch_s + ROUND_SECONDS) * 1000
    open_ts_ms = _extract_ts_ms(market, _START_DATE_FIELDS, fallback_open_ms)
    close_ts_ms = _extract_ts_ms(market, _END_DATE_FIELDS, fallback_close_ms)

    return RoundMarket(
        round_id=round_slug(start_epoch_s),
        condition_id=str(condition_id),
        token_ids=token_ids,
        open_ts_ms=open_ts_ms,
        close_ts_ms=close_ts_ms,
        raw=event,
        discovery_method=discovery_method,
    )


def parse_event_response(events: list, start_epoch_s: int) -> Optional[RoundMarket]:
    """Gamma /events?slug=... yanitini RoundMarket'e cevirir.

    Bos liste -> market bulunamadi (None). `raw` her zaman event'in kendisi,
    degistirilmeden.
    """
    if not events:
        return None
    return _build_round_market(events[0], start_epoch_s, discovery_method="slug")


async def fetch_round_market(client: httpx.AsyncClient, start_epoch_s: int) -> Optional[RoundMarket]:
    """Hizli yol: dogrudan slug ile sorgular."""
    slug = round_slug(start_epoch_s)
    response = await client.get(
        f"{GAMMA_BASE_URL}{GAMMA_EVENTS_PATH}",
        params={"slug": slug},
    )
    response.raise_for_status()
    events = response.json()
    return parse_event_response(events, start_epoch_s)


async def fetch_round_market_via_listing(
    client: httpx.AsyncClient,
    start_epoch_s: int,
    *,
    page_size: int = _LISTING_PAGE_SIZE,
    max_pages: int = _LISTING_MAX_PAGES,
) -> Optional[RoundMarket]:
    """Yedek yol: aktif/kapanmamis event'leri sayfalayarak listeler,
    `btc-updown-5m-` on ekiyle baslayan slug'lar arasindan close_ts'i
    bizim hedefimize (start_epoch_s + 300s) esit olani secer.

    Sadece resmi olarak dogrulanmis Gamma parametreleri kullanilir
    (active, closed, order, ascending, limit, offset) -- dogrulanmamis
    bir seri/tag filtresi varsayilmiyor (bkz. modul docstring'i).
    """
    target_close_ms = (start_epoch_s + ROUND_SECONDS) * 1000
    offset = 0

    for _page in range(max_pages):
        response = await client.get(
            f"{GAMMA_BASE_URL}{GAMMA_EVENTS_PATH}",
            params={
                "active": "true",
                "closed": "false",
                "order": "startDate",
                "ascending": "true",
                "limit": page_size,
                "offset": offset,
            },
        )
        response.raise_for_status()
        events = response.json()
        if not events:
            break

        for event in events:
            slug = event.get("slug") or ""
            candidate_epoch = _epoch_from_slug(slug)
            if candidate_epoch is None:
                continue

            candidate_market = _select_market_object(event)
            candidate_close_ms = _extract_ts_ms(
                candidate_market, _END_DATE_FIELDS, (candidate_epoch + ROUND_SECONDS) * 1000
            )
            if candidate_close_ms == target_close_ms:
                return _build_round_market(event, start_epoch_s, discovery_method="listing")

        if len(events) < page_size:
            break
        offset += page_size

    return None


async def discover_round_market(client: httpx.AsyncClient, start_epoch_s: int) -> Optional[RoundMarket]:
    """Once slug ile dener (hizli yol); bulunamazsa VEYA hata verirse
    listeleme + close_ts eslesmesine duser (bkz. modul docstring'i).
    Hizli yoldaki bir hata (ag, parse) sessizce yedek yola devrediyor --
    yalnizca yedek yol da basarisiz olursa cagirana hata/None gorunur.
    Hangi yolun kullanildigi donen RoundMarket.discovery_method'ta durur.
    """
    try:
        market = await fetch_round_market(client, start_epoch_s)
    except Exception:  # noqa: BLE001 -- hizli yol hatasi yedek yola dusurur
        market = None
    if market is not None:
        return market
    return await fetch_round_market_via_listing(client, start_epoch_s)
