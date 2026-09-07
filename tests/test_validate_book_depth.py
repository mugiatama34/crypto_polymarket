import copy

from validator.core import validate
from tests.fixtures import VALID_ROUND


def _up_side(record):
    return record["observations"][0]["book"]["up"]


def test_empty_depth_arrays_are_valid():
    record = copy.deepcopy(VALID_ROUND)
    side = _up_side(record)
    # best_bid/best_ask kalir ama karsilastirilacak seviye yok -> tutarlilik
    # kontrolu atlanir, sadece bos dizi olarak gecerli olmali.
    side["bids_top5"] = []
    side["asks_top5"] = []
    ok, errors = validate(record, "round")
    assert ok is True
    assert errors == []


def test_three_level_depth_is_valid():
    record = copy.deepcopy(VALID_ROUND)
    side = _up_side(record)
    side["bids_top5"] = [[0.51, 120.0], [0.50, 80.0], [0.49, 50.0]]
    ok, errors = validate(record, "round")
    assert ok is True
    assert errors == []


def test_five_level_depth_is_valid():
    record = copy.deepcopy(VALID_ROUND)
    side = _up_side(record)
    side["bids_top5"] = [
        [0.51, 120.0],
        [0.50, 80.0],
        [0.49, 50.0],
        [0.48, 30.0],
        [0.47, 10.0],
    ]
    ok, errors = validate(record, "round")
    assert ok is True
    assert errors == []


def test_more_than_five_levels_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    side = _up_side(record)
    side["bids_top5"] = [
        [0.51, 120.0],
        [0.50, 80.0],
        [0.49, 50.0],
        [0.48, 30.0],
        [0.47, 10.0],
        [0.46, 5.0],
    ]
    ok, errors = validate(record, "round")
    assert ok is False


def test_best_bid_inconsistent_with_bids_top5_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    side = _up_side(record)
    side["best_bid"] = 0.60  # bids_top5[0][0] (0.51) ile uyusmuyor
    ok, errors = validate(record, "round")
    assert ok is False
    assert any("best_bid" in e for e in errors)


def test_bid_size_inconsistent_with_bids_top5_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    side = _up_side(record)
    side["bid_size"] = 999.0  # bids_top5[0][1] (120.0) ile uyusmuyor
    ok, errors = validate(record, "round")
    assert ok is False
    assert any("bid_size" in e for e in errors)


def test_best_ask_inconsistent_with_asks_top5_is_rejected():
    record = copy.deepcopy(VALID_ROUND)
    side = _up_side(record)
    side["best_ask"] = 0.99  # asks_top5[0][0] (0.53) ile uyusmuyor
    ok, errors = validate(record, "round")
    assert ok is False
    assert any("best_ask" in e for e in errors)
