"""Gamma API'den deterministik slug ile market kesfi.

Round zamanlamasi 300s'e hizali ve onceden bilinir (round_calendar), bu
yuzden seri/tag enumerasyonu yerine dogrudan slug ile sorgulanir:
GET {GAMMA_BASE_URL}/events?slug=btc-updown-5m-<epoch>.

Slug deseni (SCHEMA.md round_id ornegiyle tutarli) birincil Polymarket
kaynagindan tam teyit edilemedi. Yanlissa istek bos liste dondurur, market
bulunamaz sayilir; cagiran taraf (sampler/runner) round'u
`status: "missed"` ile yazar, sessizce dusurulmez.

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


def _parse_json_array_field(market: dict, field: str) -> list:
    value = market.get(field)
    if isinstance(value, str):
        return json.loads(value)
    if isinstance(value, list):
        return value
    raise MarketParseError(f"'{field}' alani bulunamadi veya beklenmeyen tipte", market)


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
    for field in fields:
        raw_value = market.get(field)
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


def parse_event_response(events: list, start_epoch_s: int) -> Optional[RoundMarket]:
    """Gamma /events?slug=... yanitini RoundMarket'e cevirir.

    Bos liste -> market bulunamadi (None). `raw` her zaman event'in kendisi,
    degistirilmeden.
    """
    if not events:
        return None

    event = events[0]
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
    )


async def fetch_round_market(client: httpx.AsyncClient, start_epoch_s: int) -> Optional[RoundMarket]:
    slug = round_slug(start_epoch_s)
    response = await client.get(
        f"{GAMMA_BASE_URL}{GAMMA_EVENTS_PATH}",
        params={"slug": slug},
    )
    response.raise_for_status()
    events = response.json()
    return parse_event_response(events, start_epoch_s)
