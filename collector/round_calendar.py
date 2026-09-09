"""300 saniyeye hizali round takvimi: slug, offset -> hedef zaman.

SCHEMA.md bolum 3: offsetler "turun kapanisina kalan saniye" cinsinden.
Round'lar epoch'un 300s'e bolunebildigi noktalarda baslar (bkz. round_id
ornegi tests/fixtures.py: btc-updown-5m-1717000000).
"""

ROUND_SECONDS = 300

# SCHEMA.md bolum 3 — capa: 240,180; yogun: 135..105; capa: 60,30,10.
OFFSETS_SEC = (240, 180, 135, 130, 125, 120, 115, 110, 105, 60, 30, 10)


def round_start_epoch_s(after_epoch_s: float) -> int:
    """`after_epoch_s`'i iceren veya ondan sonraki ilk 300s-hizali baslangici dondurur."""
    floor_start = int(after_epoch_s) - (int(after_epoch_s) % ROUND_SECONDS)
    if floor_start <= after_epoch_s < floor_start + ROUND_SECONDS:
        return floor_start
    return floor_start + ROUND_SECONDS


def next_round_start_epoch_s(after_epoch_s: float) -> int:
    """`after_epoch_s`'den kesinlikle sonraki ilk 300s-hizali baslangici dondurur."""
    current = round_start_epoch_s(after_epoch_s)
    if current <= after_epoch_s:
        return current + ROUND_SECONDS
    return current


def round_slug(start_epoch_s: int) -> str:
    return f"btc-updown-5m-{start_epoch_s}"


def offset_target_ts_ms(close_ts_ms: int, offset_sec: int) -> int:
    """`offset_sec` icin hedeflenen mutlak zaman (epoch ms). close_ts - offset."""
    return close_ts_ms - offset_sec * 1000


# K-35: status (cagrilar basarili mi) ile timing_valid (zamaninda mi
# ornekelendi) ayri kavramlar -- bkz. docs/decisions.md K-34/K-35.
TIMING_VALID_TOLERANCE_SEC = 30


def is_timing_valid(observations: list) -> bool:
    """Turun tum gozlemlerinde offset_actual_sec, offset_sec'e
    `TIMING_VALID_TOLERANCE_SEC` icinde mi. Backlog nedeniyle saatler
    sonra ornekelenen turlar (K-34) burada False doner -- REST cagrilari
    basarili olsa (status: "complete") bile."""
    for o in observations:
        offset_sec = o.get("offset_sec")
        offset_actual_sec = o.get("offset_actual_sec")
        if offset_sec is None or offset_actual_sec is None:
            return False
        if abs(offset_actual_sec - offset_sec) > TIMING_VALID_TOLERANCE_SEC:
            return False
    return True
