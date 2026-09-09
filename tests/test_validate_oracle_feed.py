import copy

from validator.core import validate
from tests.fixtures import VALID_ROUND


def _obs(record):
    return record["observations"][0]


def test_valid_oracle_feed_passes():
    ok, errors = validate(copy.deepcopy(VALID_ROUND), "round")
    assert ok is True
    assert errors == []


def test_null_value_with_source_none_and_venue_none_is_valid():
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["btc_reference"] = {
        "value": None,
        "source": "none",
        "venue": "none",
        "feed_ts": None,
        "feed_ts_source": "none",
    }
    ok, errors = validate(record, "round")
    assert ok is True
    assert errors == []


def test_value_present_with_source_none_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["btc_reference"] = {
        "value": 67000.0,
        "source": "none",
        "venue": "binance",
        "feed_ts": 1717000060000,
        "feed_ts_source": "point",
    }
    ok, errors = validate(record, "round")
    assert ok is False


def test_value_present_with_venue_none_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["btc_reference"] = {
        "value": 67000.0,
        "source": "rest_poll",
        "venue": "none",
        "feed_ts": None,
        "feed_ts_source": "none",
    }
    ok, errors = validate(record, "round")
    assert ok is False


def test_unknown_source_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["btc_oracle"] = {
        "value": 67000.0,
        "source": "binance_ws",
        "venue": "chainlink",
        "feed_ts": 1717000060000,
        "feed_ts_source": "point",
    }
    ok, errors = validate(record, "round")
    assert ok is False


def test_unknown_venue_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["btc_oracle"] = {
        "value": 67000.0,
        "source": "rtds_chainlink",
        "venue": "some_other_oracle",
        "feed_ts": 1717000060000,
        "feed_ts_source": "point",
    }
    ok, errors = validate(record, "round")
    assert ok is False


def test_null_value_with_source_none_and_nonnull_feed_ts_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["btc_reference"] = {
        "value": None,
        "source": "none",
        "venue": "none",
        "feed_ts": 1234,
        "feed_ts_source": "point",
    }
    ok, errors = validate(record, "round")
    assert ok is False


def test_value_present_with_null_feed_ts_is_valid():
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["btc_oracle"] = {
        "value": 67000.0,
        "source": "rest_poll",
        "venue": "binance",
        "feed_ts": None,
        "feed_ts_source": "none",
    }
    ok, errors = validate(record, "round")
    assert ok is True
    assert errors == []


def test_missing_venue_field_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["btc_reference"] = {
        "value": None,
        "source": "none",
        "feed_ts": None,
        "feed_ts_source": "none",
    }
    ok, errors = validate(record, "round")
    assert ok is False


def test_missing_feed_ts_source_field_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["btc_reference"] = {
        "value": None,
        "source": "none",
        "venue": "none",
        "feed_ts": None,
    }
    ok, errors = validate(record, "round")
    assert ok is False


def test_unknown_feed_ts_source_value_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["btc_reference"] = {
        "value": None,
        "source": "none",
        "venue": "none",
        "feed_ts": None,
        "feed_ts_source": "computed",
    }
    ok, errors = validate(record, "round")
    assert ok is False


def test_null_feed_ts_with_feed_ts_source_point_is_rejected():
    """K-29: feed_ts null iken feed_ts_source "point" olamaz -- tek
    dogrulanmis feed_ts kaynagi payload.data[]'daki nokta, ve nokta
    varsa feed_ts zaten dolu olur (bkz. docs/decisions.md K-08 guncellemesi)."""
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["btc_reference"] = {
        "value": 67000.0,
        "source": "rtds_binance",
        "venue": "polymarket_rtds",
        "feed_ts": None,
        "feed_ts_source": "point",
    }
    ok, errors = validate(record, "round")
    assert ok is False


def test_nonnull_feed_ts_with_feed_ts_source_none_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    _obs(record)["btc_reference"] = {
        "value": 67000.0,
        "source": "rtds_binance",
        "venue": "polymarket_rtds",
        "feed_ts": 1717000060000,
        "feed_ts_source": "none",
    }
    ok, errors = validate(record, "round")
    assert ok is False
