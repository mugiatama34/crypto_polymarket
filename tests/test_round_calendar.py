from collector.round_calendar import (
    OFFSETS_SEC,
    ROUND_SECONDS,
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
