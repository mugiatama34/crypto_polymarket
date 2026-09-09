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


def test_job_end_with_discovery_counters_passes():
    record = copy.deepcopy(VALID_HEARTBEAT)
    record["event"] = "job_end"
    record["rounds_seen"] = 12
    record["rounds_missed"] = 1
    record["discovery_slug_hits"] = 11
    record["discovery_listing_hits"] = 1
    ok, errors = validate(record, "heartbeat")
    assert ok is True
    assert errors == []


def test_non_job_end_without_discovery_counters_passes():
    # discovery_slug_hits/discovery_listing_hits opsiyonel -- VALID_HEARTBEAT
    # (event: tick) zaten bu alanlar olmadan gecerli, K-21.
    ok, errors = validate(copy.deepcopy(VALID_HEARTBEAT), "heartbeat")
    assert ok is True
    assert "discovery_slug_hits" not in VALID_HEARTBEAT


def test_discovery_slug_hits_wrong_type_fails():
    record = copy.deepcopy(VALID_HEARTBEAT)
    record["event"] = "job_end"
    record["rounds_seen"] = 1
    record["rounds_missed"] = 0
    record["discovery_slug_hits"] = "1"
    record["discovery_listing_hits"] = 0
    ok, errors = validate(record, "heartbeat")
    assert ok is False


def test_discovery_counters_present_on_tick_is_still_valid():
    # Semada event'e gore kosullu bir kisit yok -- tasarim gerekcesi
    # K-21: kod bunu hic uretmiyor ama semanin kendisi engellemiyor.
    record = copy.deepcopy(VALID_HEARTBEAT)
    record["discovery_slug_hits"] = 3
    record["discovery_listing_hits"] = 0
    ok, errors = validate(record, "heartbeat")
    assert ok is True
    assert errors == []


def test_job_end_with_rounds_error_and_clob_ws_dropped_counters_passes():
    # K-32: rounds_error (rounds_missed'den ayri) + K-25'in CLOB WS
    # karsiligi olan uc dropped sayaci -- rtds_dropped_* ile ayni desen.
    record = copy.deepcopy(VALID_HEARTBEAT)
    record["event"] = "job_end"
    record["rounds_seen"] = 5
    record["rounds_missed"] = 1
    record["rounds_error"] = 2
    record["clob_ws_dropped_not_json"] = 1
    record["clob_ws_dropped_unknown_event_type"] = 3
    record["clob_ws_dropped_unknown_shape"] = 0
    ok, errors = validate(record, "heartbeat")
    assert ok is True
    assert errors == []


def test_rounds_error_wrong_type_fails():
    record = copy.deepcopy(VALID_HEARTBEAT)
    record["event"] = "job_end"
    record["rounds_seen"] = 1
    record["rounds_missed"] = 0
    record["rounds_error"] = "2"
    ok, errors = validate(record, "heartbeat")
    assert ok is False
