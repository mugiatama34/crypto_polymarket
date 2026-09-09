from collector.round_calendar import (
    OFFSETS_SEC,
    ROUND_SECONDS,
    TIMING_VALID_TOLERANCE_SEC,
    is_timing_valid,
    next_round_start_epoch_s,
    offset_target_ts_ms,
    round_slug,
    round_start_epoch_s,
)


ALIGNED_EPOCH = 1717000200  # 1717000200 % 300 == 0


def test_round_start_epoch_s_on_boundary():
    assert round_start_epoch_s(ALIGNED_EPOCH) == ALIGNED_EPOCH


def test_round_start_epoch_s_mid_round():
    assert round_start_epoch_s(ALIGNED_EPOCH + 150) == ALIGNED_EPOCH


def test_round_start_epoch_s_fractional():
    assert round_start_epoch_s(ALIGNED_EPOCH + 0.7) == ALIGNED_EPOCH


def test_next_round_start_epoch_s_advances_past_current():
    assert next_round_start_epoch_s(ALIGNED_EPOCH) == ALIGNED_EPOCH + ROUND_SECONDS


def test_next_round_start_epoch_s_mid_round():
    assert next_round_start_epoch_s(ALIGNED_EPOCH + 150) == ALIGNED_EPOCH + ROUND_SECONDS


def test_round_slug_format():
    assert round_slug(1717000000) == "btc-updown-5m-1717000000"


def test_offset_target_ts_ms():
    close_ts_ms = 1717000300000
    assert offset_target_ts_ms(close_ts_ms, 60) == 1717000240000


def test_offsets_are_descending_and_within_round():
    assert list(OFFSETS_SEC) == sorted(OFFSETS_SEC, reverse=True)
    assert all(0 < o <= ROUND_SECONDS for o in OFFSETS_SEC)


def test_is_timing_valid_true_when_all_within_tolerance():
    observations = [
        {"offset_sec": 240, "offset_actual_sec": 239.8},
        {"offset_sec": 10, "offset_actual_sec": 10.0 + TIMING_VALID_TOLERANCE_SEC},
    ]
    assert is_timing_valid(observations) is True


def test_is_timing_valid_false_when_one_observation_exceeds_tolerance():
    observations = [
        {"offset_sec": 240, "offset_actual_sec": 239.8},
        {"offset_sec": 10, "offset_actual_sec": 10.0 + TIMING_VALID_TOLERANCE_SEC + 0.1},
    ]
    assert is_timing_valid(observations) is False


def test_is_timing_valid_false_for_backlog_deviation():
    """K-34 ornegi: saatlerce gec ornekelenen tur -- buyuk negatif sapma."""
    observations = [{"offset_sec": 240, "offset_actual_sec": -19011.412}]
    assert is_timing_valid(observations) is False


def test_is_timing_valid_false_when_offset_actual_sec_missing():
    assert is_timing_valid([{"offset_sec": 240, "offset_actual_sec": None}]) is False


def test_is_timing_valid_true_for_empty_observations():
    assert is_timing_valid([]) is True
