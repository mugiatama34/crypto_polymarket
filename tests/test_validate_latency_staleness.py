"""K-20: latency_ms (ag turu) ve staleness_ms (veri tazeligi) ayri
alanlardir, ikisi de her zaman zorunlu (null yazilir, atlanmaz)."""

import copy

from validator.core import validate
from tests.fixtures import VALID_ROUND


def _obs(record):
    return record["observations"][0]


def test_ws_latency_null_staleness_filled_is_accepted():
    record = copy.deepcopy(VALID_ROUND)
    obs = _obs(record)
    obs["transport"] = "ws"
    obs["latency_ms"] = None
    obs["staleness_ms"] = 120

    ok, errors = validate(record, "round")
    assert ok is True
    assert errors == []


def test_rest_latency_filled_staleness_null_is_accepted():
    record = copy.deepcopy(VALID_ROUND)
    obs = _obs(record)
    obs["transport"] = "rest"
    obs["latency_ms"] = 85
    obs["staleness_ms"] = None

    ok, errors = validate(record, "round")
    assert ok is True
    assert errors == []


def test_both_null_is_accepted():
    record = copy.deepcopy(VALID_ROUND)
    obs = _obs(record)
    obs["latency_ms"] = None
    obs["staleness_ms"] = None

    ok, errors = validate(record, "round")
    assert ok is True
    assert errors == []


def test_both_filled_is_accepted():
    record = copy.deepcopy(VALID_ROUND)
    obs = _obs(record)
    obs["latency_ms"] = 42
    obs["staleness_ms"] = 300

    ok, errors = validate(record, "round")
    assert ok is True
    assert errors == []


def test_missing_latency_ms_key_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    del _obs(record)["latency_ms"]

    ok, errors = validate(record, "round")
    assert ok is False
    assert any("latency_ms" in e for e in errors)


def test_missing_staleness_ms_key_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    del _obs(record)["staleness_ms"]

    ok, errors = validate(record, "round")
    assert ok is False
    assert any("staleness_ms" in e for e in errors)


def test_non_integer_latency_ms_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["latency_ms"] = "40"

    ok, errors = validate(record, "round")
    assert ok is False
