"""SCHEMA.md bolum 2 dizin desenine gore append-only JSONL yazici.

Her satir yazilmadan once validator.core.validate'den gecer. Gecemeyen
satir data/rejected/'a gider, dusurulmez (SCHEMA.md bolum 7, CLAUDE.md
degismez kural 5). Ham satir hicbir sekilde yeniden serilestirilmeden
oldugu gibi write_rejected'e verilir.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from validator.core import validate
from validator.reject import write_rejected

DEFAULT_RAW_BASE_DIR = Path("data/raw")
DEFAULT_COVERAGE_BASE_DIR = Path("data/coverage")


def _date_str_from_ts_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def _append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _write_or_reject(
    record: dict,
    *,
    record_type: str,
    filename: str,
    ts_field: str,
    base_dir: Path,
    rejected_base_dir: Optional[Path],
) -> Path:
    ok, errors = validate(record, record_type)
    if not ok:
        raw_line = json.dumps(record, ensure_ascii=False)
        kwargs = {}
        if rejected_base_dir is not None:
            kwargs["base_dir"] = rejected_base_dir
        return write_rejected(
            raw_line,
            errors,
            runner_id=record.get("runner_id", "unknown"),
            record_type=record_type,
            job_id=record.get("job_id"),
            **kwargs,
        )

    date_str = _date_str_from_ts_ms(record[ts_field])
    path = Path(base_dir) / f"runner={record['runner_id']}" / f"date={date_str}" / filename
    _append_jsonl(path, record)
    return path


def write_round(
    record: dict,
    *,
    base_dir: Path = DEFAULT_RAW_BASE_DIR,
    rejected_base_dir: Optional[Path] = None,
) -> Path:
    return _write_or_reject(
        record,
        record_type="round",
        filename="rounds.jsonl",
        ts_field="open_ts",
        base_dir=base_dir,
        rejected_base_dir=rejected_base_dir,
    )


def write_heartbeat(
    record: dict,
    *,
    base_dir: Path = DEFAULT_COVERAGE_BASE_DIR,
    rejected_base_dir: Optional[Path] = None,
) -> Path:
    return _write_or_reject(
        record,
        record_type="heartbeat",
        filename="heartbeat.jsonl",
        ts_field="ts",
        base_dir=base_dir,
        rejected_base_dir=rejected_base_dir,
    )
