import copy

from validator.core import validate
from tests.fixtures import VALID_ROUND


def test_ws_observation_is_valid():
    record = copy.deepcopy(VALID_ROUND)
    record["observations"][0]["transport"] = "ws"
    ok, errors = validate(record, "round")
    assert ok is True
    assert errors == []


def test_rest_observation_is_valid():
    record = copy.deepcopy(VALID_ROUND)
    record["observations"][0]["transport"] = "rest"
    ok, errors = validate(record, "round")
    assert ok is True
    assert errors == []


def test_same_offset_sec_with_two_transports_is_accepted():
    record = copy.deepcopy(VALID_ROUND)
    ws_obs = record["observations"][0]
    ws_obs["transport"] = "ws"
    rest_obs = copy.deepcopy(ws_obs)
    rest_obs["transport"] = "rest"
    record["observations"] = [ws_obs, rest_obs]
    ok, errors = validate(record, "round")
    assert ok is True
    assert errors == []


def test_missing_transport_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    del record["observations"][0]["transport"]
    ok, errors = validate(record, "round")
    assert ok is False
    assert any("transport" in e for e in errors)


def test_unknown_transport_value_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    record["observations"][0]["transport"] = "http"
    ok, errors = validate(record, "round")
    assert ok is False
    assert any("transport" in e for e in errors)
