import copy

from validator.core import validate
from tests.fixtures import VALID_ROUND


def _obs(record):
    return record["observations"][0]


def test_valid_oracle_feed_passes():
    ok, errors = validate(copy.deepcopy(VALID_ROUND), "round")
    assert ok is True
    assert errors == []


def test_null_value_with_source_none_is_valid():
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["btc_binance"] = {"value": None, "source": "none", "feed_ts": None}
    ok, errors = validate(record, "round")
    assert ok is True
    assert errors == []


def test_value_present_with_source_none_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["btc_binance"] = {"value": 67000.0, "source": "none", "feed_ts": 1717000060000}
    ok, errors = validate(record, "round")
    assert ok is False


def test_unknown_source_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["btc_oracle"] = {
        "value": 67000.0,
        "source": "binance_ws",
        "feed_ts": 1717000060000,
    }
    ok, errors = validate(record, "round")
    assert ok is False


def test_null_value_with_source_none_and_nonnull_feed_ts_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["btc_binance"] = {"value": None, "source": "none", "feed_ts": 1234}
    ok, errors = validate(record, "round")
    assert ok is False


def test_value_present_with_null_feed_ts_is_valid():
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["btc_oracle"] = {"value": 67000.0, "source": "rest_poll", "feed_ts": None}
    ok, errors = validate(record, "round")
    assert ok is True
    assert errors == []
