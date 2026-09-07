"""SCHEMA.md sozlesmesine karsi kayit dogrulama.

Uc kayit tipi: round, outcome, heartbeat. Her biri schemas/*.schema.json
altinda ayri tanimlanir. Dogrulama katidir: bilinmeyen alan, eksik zorunlu
alan, yanlis tip veya yanlis schema_version reddedilir.
"""

import json
from pathlib import Path

from jsonschema import Draft7Validator

_SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schemas"

_SCHEMA_FILES = {
    "round": "round.schema.json",
    "outcome": "outcome.schema.json",
    "heartbeat": "heartbeat.schema.json",
}


def _load_validator(record_type: str) -> Draft7Validator:
    schema_path = _SCHEMA_DIR / _SCHEMA_FILES[record_type]
    with schema_path.open("r", encoding="utf-8") as f:
        schema = json.load(f)
    return Draft7Validator(schema)


_VALIDATORS = {record_type: _load_validator(record_type) for record_type in _SCHEMA_FILES}


def _book_side_consistency_errors(record: dict) -> list[str]:
    """best_bid/bid_size <-> bids_top5[0], best_ask/ask_size <-> asks_top5[0].

    JSON Schema (draft-07) tarafinda cross-field karsilastirma ifade
    edilemedigi icin bu kontrol schema dogrulamasi gectikten sonra ayrica
    yapilir. Dizi bossa karsilastirma atlanir (SCHEMA.md 4.1.1).
    """
    errors = []
    for i, obs in enumerate(record.get("observations", [])):
        book = obs.get("book", {})
        for side in ("up", "down"):
            b = book.get(side)
            if not isinstance(b, dict):
                continue
            bids = b.get("bids_top5") or []
            if bids and (b.get("best_bid") != bids[0][0] or b.get("bid_size") != bids[0][1]):
                errors.append(
                    f"observations/{i}/book/{side}: best_bid/bid_size does not match bids_top5[0]"
                )
            asks = b.get("asks_top5") or []
            if asks and (b.get("best_ask") != asks[0][0] or b.get("ask_size") != asks[0][1]):
                errors.append(
                    f"observations/{i}/book/{side}: best_ask/ask_size does not match asks_top5[0]"
                )
    return errors


def validate(record: dict, record_type: str) -> tuple[bool, list[str]]:
    """SCHEMA.md'ye gore record'u dogrular.

    Returns (ok, errors). errors bos ise ok True'dur.
    """
    if record_type not in _VALIDATORS:
        return False, [f"unknown record_type: {record_type!r}"]

    validator = _VALIDATORS[record_type]
    errors = sorted(validator.iter_errors(record), key=str)
    if errors:
        messages = [
            f"{'/'.join(str(p) for p in e.path) or '<root>'}: {e.message}"
            for e in errors
        ]
        return False, messages

    if record_type == "round":
        consistency_errors = _book_side_consistency_errors(record)
        if consistency_errors:
            return False, consistency_errors

    return True, []
