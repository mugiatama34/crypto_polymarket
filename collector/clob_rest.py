"""CLOB REST siparis defteri (`rest` bacagi).

GET {CLOB_REST_BASE_URL}/book?token_id=<id> -- kaynak: py-clob-client
(py_clob_client/endpoints.py GET_ORDER_BOOK, client.py get_order_book).
"""

from dataclasses import dataclass
from typing import Optional

import httpx

from .book_transform import build_book_side
from .endpoints import CLOB_BOOK_PATH, CLOB_REST_BASE_URL


@dataclass
class RestBookResult:
    book_side: dict
    venue_ts_ms: Optional[int]
    raw: dict


def _parse_venue_ts(raw: dict) -> Optional[int]:
    value = raw.get("timestamp")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


async def fetch_book(client: httpx.AsyncClient, token_id: str) -> RestBookResult:
    response = await client.get(
        f"{CLOB_REST_BASE_URL}{CLOB_BOOK_PATH}",
        params={"token_id": token_id},
    )
    response.raise_for_status()
    raw = response.json()
    book_side = build_book_side(raw.get("bids"), raw.get("asks"))
    return RestBookResult(book_side=book_side, venue_ts_ms=_parse_venue_ts(raw), raw=raw)
