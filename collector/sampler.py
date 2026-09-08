"""Offset basina iki gozlem uretir: `transport: "ws"` ve `transport: "rest"`.

SCHEMA.md 4.1 + docs/decisions.md K-10/K-19/K-20 ile tutarli. Ikisi de
asla exception firlatmaz -- eksik veri `status: "partial"/"missed"/"error"`
ile yazilir, atlanmaz (bkz. CLAUDE.md degismez kural 4).
"""

import asyncio
import time
from typing import Callable, Optional

from . import clob_rest, exchange_probe
from .book_transform import build_book_side
from .endpoints import RTDS_TOPIC_BINANCE, RTDS_TOPIC_CHAINLINK


def _empty_book_side() -> dict:
    return build_book_side([], [])


def _default_now_ms() -> int:
    return int(time.time() * 1000)


def _empty_oracle_feed() -> dict:
    return {"value": None, "source": "none", "venue": "none", "feed_ts": None}


def _offset_actual_sec(close_ts_ms: int, fired_at_ms: int) -> float:
    return (close_ts_ms - fired_at_ms) / 1000.0


def _staleness_ms(response_ts: int, reference_feed: dict) -> Optional[int]:
    """K-20: response_ts - btc_reference.feed_ts. feed_ts yoksa null.

    btc_reference secildi cunku karar bu fiyata bakilarak veriliyor
    (K-09) -- operasyonel olarak en ilgili olan bu. btc_oracle.feed_ts
    ayri, kendi amaci icin duruyor (K-10), burada karistirilmiyor.
    """
    feed_ts = reference_feed.get("feed_ts")
    if feed_ts is None:
        return None
    return response_ts - feed_ts


async def build_ws_observation(
    *,
    offset_sec: int,
    close_ts_ms: int,
    token_ids: dict,
    rtds_client,
    clob_ws_client,
    now_ms_fn: Optional[Callable[[], int]] = None,
) -> tuple:
    """Bellekteki WS cache'inden anlik kopya.

    `latency_ms` burada AG TURU DEGIL -- cache okuma/kopyalama suresidir
    (tipik olarak sub-ms, pratikte hep 0'a yuvarlanir), bu yuzden K-20
    geregi `null` yazilir; `rest` bacaginin latency_ms'iyle karistirilmaz.
    Veri tazeligi `staleness_ms` ile ayri olculur (response_ts -
    btc_reference.feed_ts).

    Donen raw listesi RTDS'in ham zarflarini tasir (varsa) --
    `rtds_client.snapshot()`'in `raw_envelope` alani, `publish_ts_ms`
    gibi ayiklanmis alanlarin kaynaklandigi ham mesaj. CLOB WS defter
    anlik goruntusu icin ayri bir ham kayit YOK (bu fazda kapsam disi);
    yalnizca RTDS icin eklendi.
    """
    now_ms = now_ms_fn or _default_now_ms
    fired_at_ms = now_ms()

    runner_ts = now_ms()
    up_ws = clob_ws_client.snapshot(token_ids["up"])
    down_ws = clob_ws_client.snapshot(token_ids["down"])
    binance_ws = rtds_client.snapshot(RTDS_TOPIC_BINANCE)
    chainlink_ws = rtds_client.snapshot(RTDS_TOPIC_CHAINLINK)
    response_ts = now_ms()  # cache kopyalama bitti -- ag cagrisi yok

    missing = []
    if up_ws is None:
        missing.append("book.up")
    if down_ws is None:
        missing.append("book.down")
    if binance_ws is None:
        missing.append("btc_reference")
    if chainlink_ws is None:
        missing.append("btc_oracle")

    if len(missing) == 4:
        status = "missed"
    elif missing:
        status = "partial"
    else:
        status = "ok"

    book = {
        "up": up_ws["book_side"] if up_ws else _empty_book_side(),
        "down": down_ws["book_side"] if down_ws else _empty_book_side(),
    }

    if up_ws is not None:
        venue_ts = up_ws["venue_ts_ms"]
    elif down_ws is not None:
        venue_ts = down_ws["venue_ts_ms"]
    else:
        # Defter cache'i hic doluymamis -- gercek bir venue zaman damgasi
        # yok. runner_ts'e dusuluyor (schema venue_ts'i nullable degil);
        # status zaten "missed"/"partial" oldugu icin bu deger guvenilir
        # sayilmamali.
        venue_ts = runner_ts
    if venue_ts is None:
        venue_ts = runner_ts

    btc_reference = (
        {
            "value": binance_ws["value"],
            "source": "rtds_binance",
            "venue": "polymarket_rtds",
            "feed_ts": binance_ws["feed_ts_ms"],
        }
        if binance_ws
        else _empty_oracle_feed()
    )
    btc_oracle = (
        {
            "value": chainlink_ws["value"],
            "source": "rtds_chainlink",
            "venue": "chainlink",
            "feed_ts": chainlink_ws["feed_ts_ms"],
        }
        if chainlink_ws
        else _empty_oracle_feed()
    )

    observation = {
        "offset_sec": offset_sec,
        "offset_actual_sec": _offset_actual_sec(close_ts_ms, fired_at_ms),
        "venue_ts": venue_ts,
        "response_ts": response_ts,
        "runner_ts": runner_ts,
        "latency_ms": None,
        "staleness_ms": _staleness_ms(response_ts, btc_reference),
        "transport": "ws",
        "book": book,
        "btc_reference": btc_reference,
        "btc_oracle": btc_oracle,
        "status": status,
        "error": ("eksik: " + ", ".join(missing)) if missing else None,
    }

    ws_raw = []
    if binance_ws is not None and binance_ws.get("raw_envelope") is not None:
        ws_raw.append({"endpoint": "rtds_binance", "payload": binance_ws["raw_envelope"]})
    if chainlink_ws is not None and chainlink_ws.get("raw_envelope") is not None:
        ws_raw.append({"endpoint": "rtds_chainlink", "payload": chainlink_ws["raw_envelope"]})
    return observation, ws_raw


async def build_rest_observation(
    *,
    offset_sec: int,
    close_ts_ms: int,
    token_ids: dict,
    http_client,
    exchange: Optional[str],
    now_ms_fn: Optional[Callable[[], int]] = None,
) -> tuple:
    """Gercek REST cagrilariyla gozlem. `latency_ms` burada gercek ag
    turu (K-20). `staleness_ms`, btc_reference.feed_ts REST'te genellikle
    donmedigi icin (K-10) fiilen hep null cikar -- bu kod bunu varsaymaz,
    feed_ts bir gun dolarsa otomatik calisir.

    `btc_oracle` bu bacakta her zaman null/"none" yazilir -- Chainlink
    icin genel-amacli, kimlik dogrulamasiz bir REST/on-chain esdegeri bu
    fazda dogrulanmadi (bkz. docs/decisions.md K-19, plan onayi). Bu
    fabrikasyon degil, bilinen bir bosluk -- status alani bunu maskelemez.
    """
    now_ms = now_ms_fn or _default_now_ms
    fired_at_ms = now_ms()
    runner_ts = now_ms()

    tasks = [
        clob_rest.fetch_book(http_client, token_ids["up"]),
        clob_rest.fetch_book(http_client, token_ids["down"]),
    ]
    if exchange is not None:
        tasks.append(exchange_probe.fetch_price(http_client, exchange))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    response_ts = now_ms()

    up_result = results[0]
    down_result = results[1]
    exch_result = results[2] if exchange is not None else None

    raw_entries = []
    errors = []
    successes = 0

    if isinstance(up_result, Exception):
        book_up = _empty_book_side()
        up_venue_ts = None
        errors.append(f"book.up: {up_result}")
    else:
        book_up = up_result.book_side
        up_venue_ts = up_result.venue_ts_ms
        raw_entries.append({"endpoint": "clob_book_up", "payload": up_result.raw})
        successes += 1

    if isinstance(down_result, Exception):
        book_down = _empty_book_side()
        down_venue_ts = None
        errors.append(f"book.down: {down_result}")
    else:
        book_down = down_result.book_side
        down_venue_ts = down_result.venue_ts_ms
        raw_entries.append({"endpoint": "clob_book_down", "payload": down_result.raw})
        successes += 1

    venue_ts = up_venue_ts if up_venue_ts is not None else down_venue_ts
    if venue_ts is None:
        venue_ts = runner_ts

    if exch_result is None:
        btc_reference = _empty_oracle_feed()
    elif isinstance(exch_result, Exception):
        btc_reference = _empty_oracle_feed()
        errors.append(f"btc_reference: {exch_result}")
    elif exch_result.value is None:
        btc_reference = _empty_oracle_feed()
        errors.append(f"btc_reference: hicbir borsadan alinamadi ({exchange})")
        raw_entries.append(
            {
                "endpoint": f"{exchange}_ticker_failed",
                "payload": {"attempts": [a.__dict__ for a in exch_result.attempts]},
            }
        )
    else:
        btc_reference = {
            "value": exch_result.value,
            "source": "rest_poll",
            "venue": exch_result.exchange,
            "feed_ts": None,
        }
        raw_entries.append({"endpoint": f"{exch_result.exchange}_ticker", "payload": exch_result.raw})
        successes += 1

    attempted = 2 + (1 if exchange is not None else 0)
    if successes == attempted:
        status = "ok"
    elif successes == 0:
        status = "error" if errors else "missed"
    else:
        status = "partial"

    observation = {
        "offset_sec": offset_sec,
        "offset_actual_sec": _offset_actual_sec(close_ts_ms, fired_at_ms),
        "venue_ts": venue_ts,
        "response_ts": response_ts,
        "runner_ts": runner_ts,
        "latency_ms": response_ts - runner_ts,
        "staleness_ms": _staleness_ms(response_ts, btc_reference),
        "transport": "rest",
        "book": {"up": book_up, "down": book_down},
        "btc_reference": btc_reference,
        "btc_oracle": _empty_oracle_feed(),
        "status": status,
        "error": "; ".join(errors) if errors else None,
    }
    return observation, raw_entries
