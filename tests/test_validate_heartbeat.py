import copy

from validator.core import validate
from tests.fixtures import VALID_HEARTBEAT


def test_valid_heartbeat_passes():
    ok, errors = validate(copy.deepcopy(VALID_HEARTBEAT), "heartbeat")
    assert ok is True
    assert errors == []


def test_missing_required_field_fails():
    record = copy.deepcopy(VALID_HEARTBEAT)
    del record["event"]
    ok, errors = validate(record, "heartbeat")
    assert ok is False
    assert any("event" in e for e in errors)


def test_wrong_type_fails():
    record = copy.deepcopy(VALID_HEARTBEAT)
    record["ts"] = "not-an-int"
    ok, errors = validate(record, "heartbeat")
    assert ok is False
    assert any("ts" in e for e in errors)


def test_unknown_field_fails():
    record = copy.deepcopy(VALID_HEARTBEAT)
    record["extra_field"] = "should not be here"
    ok, errors = validate(record, "heartbeat")
    assert ok is False
    assert any("extra_field" in e or "Additional properties" in e for e in errors)


def test_wrong_schema_version_fails():
    record = copy.deepcopy(VALID_HEARTBEAT)
    record["schema_version"] = 2
    ok, errors = validate(record, "heartbeat")
    assert ok is False
    assert any("schema_version" in e for e in errors)
