import pytest

from collector.book_transform import build_book_side


def test_build_book_side_selects_best_regardless_of_input_order():
    bids = [{"price": "0.48", "size": "10"}, {"price": "0.51", "size": "5"}]
    asks = [{"price": "0.55", "size": "3"}, {"price": "0.53", "size": "7"}]
    side = build_book_side(bids, asks)
    assert side["best_bid"] == 0.51
    assert side["bid_size"] == 5.0
    assert side["best_ask"] == 0.53
    assert side["ask_size"] == 7.0
    assert side["bids_top5"] == [[0.51, 5.0], [0.48, 10.0]]
    assert side["asks_top5"] == [[0.53, 7.0], [0.55, 3.0]]
    assert side["spread"] == pytest.approx(0.02)
    assert side["mid"] == (0.51 + 0.53) / 2


def test_build_book_side_drops_zero_size_levels():
    bids = [{"price": "0.5", "size": "0"}, {"price": "0.49", "size": "10"}]
    side = build_book_side(bids, [])
    assert side["bids_top5"] == [[0.49, 10.0]]


def test_build_book_side_caps_at_five_levels():
    bids = [{"price": str(0.4 + i * 0.01), "size": "1"} for i in range(8)]
    side = build_book_side(bids, [])
    assert len(side["bids_top5"]) == 5


def test_build_book_side_empty_both_sides_uses_zero_sentinel():
    side = build_book_side([], [])
    assert side["bids_top5"] == []
    assert side["asks_top5"] == []
    assert side["best_bid"] == 0.0
    assert side["best_ask"] == 0.0
    assert side["mid"] == 0.0


def test_build_book_side_one_side_empty():
    side = build_book_side([{"price": "0.4", "size": "1"}], [])
    assert side["best_ask"] == 0.0
    assert side["mid"] == 0.4
