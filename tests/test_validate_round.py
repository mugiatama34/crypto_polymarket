import copy

from validator.core import validate
from tests.fixtures import VALID_ROUND


def test_valid_round_passes():
    ok, errors = validate(copy.deepcopy(VALID_ROUND), "round")
    assert ok is True
    assert errors == []


def test_missing_required_field_fails():
    record = copy.deepcopy(VALID_ROUND)
    del record["condition_id"]
    ok, errors = validate(record, "round")
    assert ok is False
    assert any("condition_id" in e for e in errors)


def test_wrong_type_fails():
    record = copy.deepcopy(VALID_ROUND)
    record["open_ts"] = "not-an-int"
    ok, errors = validate(record, "round")
    assert ok is False
    assert any("open_ts" in e for e in errors)


def test_unknown_field_fails():
    record = copy.deepcopy(VALID_ROUND)
    record["extra_field"] = "should not be here"
    ok, errors = validate(record, "round")
    assert ok is False
    assert any("extra_field" in e or "Additional properties" in e for e in errors)


def test_wrong_schema_version_fails():
    record = copy.deepcopy(VALID_ROUND)
    record["schema_version"] = 3
    ok, errors = validate(record, "round")
    assert ok is False
    assert any("schema_version" in e for e in errors)


def test_old_v1_schema_version_fails():
    """K-36: schema v1 -> v2 gecisinde eski surum donusturulmuyor (K-14) --
    v1 olarak yazilmis bir satir artik v2 dogrulayicidan gecmez, bu
    kasitli (analiz katmani her iki surumu de job_id/schema_version'a
    bakarak ayri ayri okur, veri degistirilmez)."""
    record = copy.deepcopy(VALID_ROUND)
    record["schema_version"] = 1
    ok, errors = validate(record, "round")
    assert ok is False
    assert any("schema_version" in e for e in errors)
