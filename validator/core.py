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


def validate(record: dict, record_type: str) -> tuple[bool, list[str]]:
    """SCHEMA.md'ye gore record'u dogrular.

    Returns (ok, errors). errors bos ise ok True'dur.
    """
    if record_type not in _VALIDATORS:
        return False, [f"unknown record_type: {record_type!r}"]

    validator = _VALIDATORS[record_type]
    errors = sorted(validator.iter_errors(record), key=str)
    if not errors:
        return True, []

    messages = [
        f"{'/'.join(str(p) for p in e.path) or '<root>'}: {e.message}"
        for e in errors
    ]
    return False, messages
