import copy

from validator.core import validate
from tests.fixtures import VALID_OUTCOME


def test_valid_outcome_passes():
    ok, errors = validate(copy.deepcopy(VALID_OUTCOME), "outcome")
    assert ok is True
    assert errors == []


def test_missing_required_field_fails():
    record = copy.deepcopy(VALID_OUTCOME)
    del record["resolution_source"]
    ok, errors = validate(record, "outcome")
    assert ok is False
    assert any("resolution_source" in e for e in errors)


def test_wrong_type_fails():
    record = copy.deepcopy(VALID_OUTCOME)
    record["resolved_ts"] = "not-an-int"
    ok, errors = validate(record, "outcome")
    assert ok is False
    assert any("resolved_ts" in e for e in errors)


def test_unknown_field_fails():
    record = copy.deepcopy(VALID_OUTCOME)
    record["extra_field"] = "should not be here"
    ok, errors = validate(record, "outcome")
    assert ok is False
    assert any("extra_field" in e or "Additional properties" in e for e in errors)


def test_wrong_schema_version_fails():
    record = copy.deepcopy(VALID_OUTCOME)
    record["schema_version"] = 2
    ok, errors = validate(record, "outcome")
    assert ok is False
    assert any("schema_version" in e for e in errors)
