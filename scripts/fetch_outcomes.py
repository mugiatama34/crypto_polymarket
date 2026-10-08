#!/usr/bin/env python3
"""Cozulmus turlarin sonucunu (up/down) Gamma'dan tek seferlik ceken script.

Surekli calisan bir uzlastirici DEGILDIR (bkz. docs/decisions.md K-38) --
GitHub Actions'ta `workflow_dispatch` ile elle tetiklenir
(`.github/workflows/fetch_outcomes.yml`).

Davranis:

- `data/raw/runner=*/date=*/rounds.jsonl[.gz]` icindeki her `round_id`
  icin, `close_ts`'i `--min-age-sec` kadar gecmis olanlar aday olur.
- `data/outcomes/` altinda zaten yazilmis `round_id`'ler atlanir
  (append-only, K-05/K-11) -- yarida kesilirse yeniden calistirmak
  guvenlidir, ayni tur iki kez yazilmaz.
- Istekler `--min-interval-sec` (varsayilan 0.4 sn, ~2.5 istek/sn) ile
  sinirlanir. 429/5xx/ag hatasinda ustel geri cekilme ile en fazla
  `MAX_ATTEMPTS` deneme; sonra tur `fetch_error` olarak sayilir, yazilmaz.
- Sonuc yalnizca market `closed` ve `outcomePrices` kesin (1/0 veya 0/1)
  ise yazilir; 0.5/0.5 -> `invalid`. Kapanmamis veya kesin olmayan market
  YAZILMAZ, `pending_*` olarak sayilir (uydurma yok, sonra tekrar
  denenebilir).
- Her satir `validator.core.validate(..., "outcome")`'dan gecer; gecemeyen
  `data/rejected/runner=reconciler/`'a gider (CLAUDE.md degismez kural 5).
- Yazilan dosya: `data/outcomes/date=<close_ts UTC tarihi>/outcomes.jsonl`
  (SCHEMA.md bolum 2).

Hicbir sey sessizce atlanmaz: tum sayaclar stdout'a ve (Actions'ta)
`$GITHUB_STEP_SUMMARY`'ye basilir.

Kullanim:
    python -m scripts.fetch_outcomes
    python -m scripts.fetch_outcomes --limit 20 --dry-run
"""

import argparse
import gzip
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional

import httpx

from collector.endpoints import GAMMA_BASE_URL, GAMMA_EVENTS_PATH
from collector.gamma_client import _parse_iso_ms, _parse_json_array_field, _select_market_object
from validator.core import validate
from validator.reject import write_rejected

OUTCOME_SCHEMA_VERSION = 1
RESOLUTION_SOURCE = "gamma_outcome_prices"
WRITER_ID = "reconciler"

DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_OUTCOMES_DIR = Path("data/outcomes")
DEFAULT_REJECTED_DIR = Path("data/rejected")

DEFAULT_MIN_INTERVAL_SEC = 0.4
DEFAULT_MIN_AGE_SEC = 600
DEFAULT_MAX_SECONDS = 6000
MAX_ATTEMPTS = 5
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

# resolved_ts icin denenen alanlar, sirayla. Hangisinin kullanildigi
# raw.resolved_ts_field'a yazilir -- varsayim sayinin yaninda durur.
_RESOLVED_TS_FIELDS = ("closedTime", "umaEndDate", "endDate")


def _open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def _jsonl_files(base_dir: Path, pattern: str) -> list[Path]:
    files = list(base_dir.glob(pattern)) + list(base_dir.glob(pattern + ".gz"))
    return sorted(files)


def _iter_jsonl(paths: Iterable[Path]):
    for path in paths:
        with _open_text(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)


def load_candidate_rounds(raw_dir: Path, now_ms: int, min_age_sec: int) -> list[tuple[str, int]]:
    """(round_id, close_ts) listesi, close_ts sirali, tekrarlar teklenmis."""
    seen: dict[str, int] = {}
    for rec in _iter_jsonl(_jsonl_files(raw_dir, "runner=*/date=*/rounds.jsonl")):
        rid = rec.get("round_id")
        close_ts = rec.get("close_ts")
        if not isinstance(rid, str) or not isinstance(close_ts, int):
            continue
        if close_ts > now_ms - min_age_sec * 1000:
            continue
        seen.setdefault(rid, close_ts)
    return sorted(seen.items(), key=lambda kv: (kv[1], kv[0]))


def load_existing_outcome_ids(outcomes_dir: Path) -> set[str]:
    ids = set()
    for rec in _iter_jsonl(_jsonl_files(outcomes_dir, "date=*/outcomes.jsonl")):
        rid = rec.get("round_id")
        if isinstance(rid, str):
            ids.add(rid)
    return ids


def _resolved_ts(market: dict) -> tuple[Optional[int], Optional[str]]:
    for field in _RESOLVED_TS_FIELDS:
        value = market.get(field)
        if isinstance(value, str):
            parsed = _parse_iso_ms(value)
            if parsed is not None:
                return parsed, field
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            return int(value), field
    return None, None


def parse_outcome(events, round_id: str) -> tuple[str, Optional[dict]]:
    """Gamma /events?slug= yanitini (liste) yorumlar.

    Donus: (durum, kayit). Durum `written` disindaysa kayit None'dir.
    Durumlar: written, pending_not_found, pending_not_closed,
    pending_not_final, parse_error.
    """
    if not isinstance(events, list) or not events:
        return "pending_not_found", None
    event = events[0]
    if not isinstance(event, dict):
        return "parse_error", None
    market = _select_market_object(event)

    if market.get("closed") is not True:
        return "pending_not_closed", None

    try:
        labels = [str(x).strip().lower() for x in _parse_json_array_field(market, "outcomes")]
        prices = [float(x) for x in _parse_json_array_field(market, "outcomePrices")]
    except Exception:
        return "parse_error", None
    if sorted(labels) != ["down", "up"] or len(prices) != 2:
        return "parse_error", None

    by_label = dict(zip(labels, prices))
    if by_label["up"] == 1.0 and by_label["down"] == 0.0:
        outcome = "up"
    elif by_label["up"] == 0.0 and by_label["down"] == 1.0:
        outcome = "down"
    elif by_label["up"] == 0.5 and by_label["down"] == 0.5:
        outcome = "invalid"
    else:
        return "pending_not_final", None

    resolved_ts, ts_field = _resolved_ts(market)
    if resolved_ts is None:
        return "parse_error", None

    record = {
        "schema_version": OUTCOME_SCHEMA_VERSION,
        "round_id": round_id,
        "resolved_ts": resolved_ts,
        "outcome": outcome,
        "resolution_source": RESOLUTION_SOURCE,
        # Gamma bu degerleri vermiyor; uydurulmaz (K-06 ruhu).
        "open_price": None,
        "close_price": None,
        "raw": {
            "endpoint": "gamma_event_slug",
            "resolved_ts_field": ts_field,
            "payload": event,
        },
    }
    return "written", record


def _date_str(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def write_outcome(record: dict, close_ts: int, *, outcomes_dir: Path, rejected_dir: Path) -> tuple[bool, Path]:
    ok, errors = validate(record, "outcome")
    if not ok:
        path = write_rejected(
            json.dumps(record, ensure_ascii=False),
            errors,
            runner_id=WRITER_ID,
            record_type="outcome",
            base_dir=rejected_dir,
        )
        return False, path
    path = outcomes_dir / f"date={_date_str(close_ts)}" / "outcomes.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
    return True, path


class RateLimitedFetcher:
    """Istekler arasi en az `min_interval_sec`; yeniden denemeler de sayilir."""

    def __init__(
        self,
        client: httpx.Client,
        *,
        min_interval_sec: float,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.client = client
        self.min_interval_sec = min_interval_sec
        self.sleep = sleep
        self.clock = clock
        self._last: Optional[float] = None
        self.requests = 0

    def _wait(self) -> None:
        if self._last is not None:
            remaining = self.min_interval_sec - (self.clock() - self._last)
            if remaining > 0:
                self.sleep(remaining)
        self._last = self.clock()

    def fetch_event(self, slug: str):
        """Basarida JSON govdesini, tum denemeler bitince None dondurur."""
        backoff = 2.0
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self._wait()
            self.requests += 1
            try:
                resp = self.client.get(f"{GAMMA_BASE_URL}{GAMMA_EVENTS_PATH}", params={"slug": slug})
            except httpx.HTTPError:
                resp = None
            if resp is not None and resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError:
                    return None
            if resp is not None and resp.status_code not in RETRYABLE_STATUS:
                return None
            if attempt < MAX_ATTEMPTS:
                self.sleep(backoff)
                backoff *= 2
        return None


def run(
    *,
    raw_dir: Path,
    outcomes_dir: Path,
    rejected_dir: Path,
    fetcher,
    now_ms: int,
    min_age_sec: int = DEFAULT_MIN_AGE_SEC,
    max_seconds: float = DEFAULT_MAX_SECONDS,
    limit: Optional[int] = None,
    dry_run: bool = False,
    clock: Callable[[], float] = time.monotonic,
) -> Counter:
    counts: Counter = Counter()
    candidates = load_candidate_rounds(raw_dir, now_ms, min_age_sec)
    existing = load_existing_outcome_ids(outcomes_dir)
    counts["candidates"] = len(candidates)
    pending = [(rid, cts) for rid, cts in candidates if rid not in existing]
    counts["already_written"] = len(candidates) - len(pending)
    if limit is not None:
        pending = pending[:limit]

    started = clock()
    for i, (round_id, close_ts) in enumerate(pending):
        if clock() - started > max_seconds:
            counts["not_attempted_time_budget"] = len(pending) - i
            break
        events = fetcher.fetch_event(round_id)
        if events is None:
            counts["fetch_error"] += 1
            continue
        status, record = parse_outcome(events, round_id)
        if status != "written":
            counts[status] += 1
            continue
        counts[f"outcome_{record['outcome']}"] += 1
        if dry_run:
            counts["dry_run_not_written"] += 1
            continue
        ok, _ = write_outcome(record, close_ts, outcomes_dir=outcomes_dir, rejected_dir=rejected_dir)
        counts["written" if ok else "rejected"] += 1
    return counts


def _format_summary(counts: Counter, requests: int) -> str:
    lines = ["fetch_outcomes ozeti", ""]
    for key in sorted(counts):
        lines.append(f"- {key}: {counts[key]}")
    lines.append(f"- http_requests: {requests}")
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    p.add_argument("--outcomes-dir", type=Path, default=DEFAULT_OUTCOMES_DIR)
    p.add_argument("--rejected-dir", type=Path, default=DEFAULT_REJECTED_DIR)
    p.add_argument("--min-interval-sec", type=float, default=DEFAULT_MIN_INTERVAL_SEC)
    p.add_argument("--min-age-sec", type=int, default=DEFAULT_MIN_AGE_SEC)
    p.add_argument("--max-seconds", type=float, default=DEFAULT_MAX_SECONDS)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    now_ms = int(time.time() * 1000)
    with httpx.Client(timeout=15.0) as client:
        fetcher = RateLimitedFetcher(client, min_interval_sec=args.min_interval_sec)
        counts = run(
            raw_dir=args.raw_dir,
            outcomes_dir=args.outcomes_dir,
            rejected_dir=args.rejected_dir,
            fetcher=fetcher,
            now_ms=now_ms,
            min_age_sec=args.min_age_sec,
            max_seconds=args.max_seconds,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    summary = _format_summary(counts, fetcher.requests)
    print(summary)
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as f:
            f.write(summary + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
