import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts import final_analysis as fa


def _ts(y, m, d, hh=12):
    return int(datetime(y, m, d, hh, tzinfo=timezone.utc).timestamp() * 1000)


def _side(best_ask, asks=None):
    asks = asks if asks is not None else [[best_ask, 100.0]]
    return {"best_ask": best_ask, "asks_top5": asks}


def _round(rid, close_ts, up, down, *, status="ok", actual=120.0, transport="rest", sv=2, offset=120):
    return {
        "schema_version": sv,
        "round_id": rid,
        "close_ts": close_ts,
        "observations": [
            {"offset_sec": 180, "transport": "rest", "status": "ok", "offset_actual_sec": 180.0,
             "book": {"up": _side(0.5), "down": _side(0.5)}},
            {"offset_sec": offset, "transport": transport, "status": status, "offset_actual_sec": actual,
             "book": {"up": up, "down": down}},
        ],
    }


def _write(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


# --- kural adimlari -------------------------------------------------------
def test_side_selection_and_range_bounds():
    assert fa.evaluate_round(_round("r", 0, _side(0.80), _side(0.21)))[1]["side"] == "up"
    assert fa.evaluate_round(_round("r", 0, _side(0.03), _side(0.99)))[1]["side"] == "down"
    assert fa.evaluate_round(_round("r", 0, _side(0.79), _side(0.22)))[0] == "aralik_disi"
    assert fa.evaluate_round(_round("r", 0, _side(1.0), _side(0.01)))[0] == "aralik_disi"
    # ikisi de aralikta -> yuksek olan
    assert fa.evaluate_round(_round("r", 0, _side(0.85), _side(0.90)))[1]["side"] == "down"
    assert fa.evaluate_round(_round("r", 0, _side(0.85), _side(0.85)))[0] == "tie"
    assert fa.evaluate_round(_round("r", 0, _side(None, []), _side(0.9)))[1]["side"] == "down"


def test_observation_validity():
    good = (_side(0.9), _side(0.11))
    assert fa.evaluate_round(_round("r", 0, *good, status="error"))[0] == "gozlem_status"
    assert fa.evaluate_round(_round("r", 0, *good, actual=150.0))[0] == "enter"
    assert fa.evaluate_round(_round("r", 0, *good, actual=150.1))[0] == "gozlem_zamanlama"
    assert fa.evaluate_round(_round("r", 0, *good, actual=89.9))[0] == "gozlem_zamanlama"
    assert fa.evaluate_round(_round("r", 0, *good, transport="ws"))[0] == "gozlem_yok"
    assert fa.evaluate_round(_round("r", 0, *good, offset=115))[0] == "gozlem_yok"


def test_vwap_and_depth():
    # 4 @0.90 + 6 @0.92 -> (3.6 + 5.52)/10 = 0.912
    r = _round("r", 0, _side(0.90, [[0.90, 4], [0.92, 6], [0.95, 100]]), _side(0.11))
    assert fa.evaluate_round(r)[1]["entry_price"] == pytest.approx(0.912)
    # 5 seviye toplami 9.5 < 10
    thin = [[0.90, 2], [0.91, 2], [0.92, 2], [0.93, 2], [0.94, 1.5]]
    assert fa.evaluate_round(_round("r", 0, _side(0.90, thin), _side(0.11)))[0] == "derinlik_yetersiz"
    # aralik kontrolu best_ask'le: VWAP 0.99'u gecse de islem var
    r = _round("r", 0, _side(0.99, [[0.99, 5], [1.0, 5]]), _side(0.02))
    assert fa.evaluate_round(r)[1]["entry_price"] == pytest.approx(0.995)


def test_fee_and_pnl():
    t = fa.Trade("r", 0, "up", 0.9, 0.9, won=True)
    fee = 10 * 0.07 * 0.9 * 0.1  # 0.063
    assert t.fee(1.0) == pytest.approx(fee)
    assert t.pnl(0.0) == pytest.approx(1.0)
    assert t.pnl(1.0) == pytest.approx(1.0 - fee)
    assert t.pnl(2.0) == pytest.approx(1.0 - 2 * fee)
    lost = fa.Trade("r", 0, "up", 0.9, 0.9, won=False)
    assert lost.pnl(1.0) == pytest.approx(-9.0 - fee)


# --- istatistik -----------------------------------------------------------
def test_wilson_known_value():
    lo, hi = fa.wilson_interval(9, 10)
    assert lo == pytest.approx(0.5958, abs=1e-4)
    assert hi == pytest.approx(0.9821, abs=1e-4)
    assert fa.wilson_interval(0, 0) == (None, None)


def test_streak_and_drawdown():
    mk = lambda w: fa.Trade("r", 0, "up", 0.9, 0.9, won=w)
    assert fa.longest_loss_streak([mk(x) for x in [1, 0, 0, 1, 0, 0, 0, 1]]) == 3
    assert fa.max_drawdown([1, 1, -3, 1, -1, 5]) == pytest.approx(3)
    assert fa.max_drawdown([-2, 1]) == pytest.approx(2)  # zirve 0'dan baslar


def test_bootstrap_deterministic_and_degenerate():
    a = fa.bootstrap_mean_ci({1.0: [1.0, -9.0, 1.0, 1.0]})
    b = fa.bootstrap_mean_ci({1.0: [1.0, -9.0, 1.0, 1.0]})
    assert a == b
    const = fa.bootstrap_mean_ci({1.0: [0.5] * 7})[1.0]
    assert const[0] == pytest.approx(0.5) and const[1] == pytest.approx(0.5)
    assert fa.bootstrap_mean_ci({1.0: []})[1.0] == (None, None)


def test_buckets():
    assert fa._bucket_of(0.80) == "0.80-0.85"
    assert fa._bucket_of(0.8499) == "0.80-0.85"
    assert fa._bucket_of(0.85) == "0.85-0.90"
    assert fa._bucket_of(0.95) == "0.95-0.99"
    assert fa._bucket_of(0.99) == "0.95-0.99"
    assert fa._bucket_of(0.995) == "0.95-0.99"


# --- uctan uca ------------------------------------------------------------
def test_end_to_end_windows_and_criterion(tmp_path):
    raw = tmp_path / "raw" / "runner=longjob"
    out = tmp_path / "outcomes"
    up90 = (_side(0.90), _side(0.11))
    _write(raw / "date=2026-09-09" / "rounds.jsonl", [_round("v1", _ts(2026, 9, 9), *up90, sv=1)])
    _write(raw / "date=2026-09-10" / "rounds.jsonl", [
        _round("k1", _ts(2026, 9, 10), *up90),
        _round("k2", _ts(2026, 9, 10, 13), _side(0.5), _side(0.51)),  # aralik disi
    ])
    test_rows = [_round(f"t{i}", _ts(2026, 9, 24) + i * 300_000, *up90) for i in range(20)]
    test_rows.append(_round("t0", _ts(2026, 9, 24), *up90))  # tekrar
    test_rows.append(_round("tn", _ts(2026, 9, 25), *up90))  # sonuc yok
    test_rows.append(_round("ti", _ts(2026, 9, 25, 13), *up90))  # invalid
    _write(raw / "date=2026-09-24" / "rounds.jsonl", test_rows)
    _write(raw / "date=2026-10-09" / "rounds.jsonl", [_round("s1", _ts(2026, 10, 9), *up90)])
    _write(raw / "date=2026-11-01" / "rounds.jsonl", [_round("x", _ts(2026, 11, 1), *up90)])

    outcomes = [{"round_id": "k1", "outcome": "up"}, {"round_id": "v1", "outcome": "up"},
                {"round_id": "ti", "outcome": "invalid"}, {"round_id": "s1", "outcome": "up"}]
    outcomes += [{"round_id": f"t{i}", "outcome": "up"} for i in range(20)]
    _write(out / "date=2026-09-24" / "outcomes.jsonl", outcomes)

    rep = fa.build_report(tmp_path / "raw", out, include_second_window=False, now_ms=_ts(2026, 10, 10))
    assert rep["excluded_rows"] == {"schema_v1": 1, "pencere_disi": 1}
    assert set(rep["windows"]) == {"kesif", "test"}

    k = rep["windows"]["kesif"]
    assert k["rounds"] == 2 and k["trades"] == 1 and k["skipped"]["aralik_disi"] == 1

    t = rep["windows"]["test"]
    assert t["rounds"] == 22 and t["trades"] == 20
    assert t["skipped"]["tekrar"] == 1 and t["skipped"]["sonuc_yok"] == 1 and t["skipped"]["sonuc_invalid"] == 1
    assert t["skipped_total"] == 2
    assert t["win_rate"] == 1.0 and t["mean_entry_price"] == pytest.approx(0.90)
    assert t["edge"] == pytest.approx(0.10)
    fee = 10 * 0.07 * 0.9 * 0.1
    assert t["fee_sensitivity"]["1x"]["mean_net_pnl"] == pytest.approx(1.0 - fee)
    assert t["total_pnl_1x"] == pytest.approx(20 * (1.0 - fee))
    assert t["max_drawdown_1x"] == 0 and t["longest_loss_streak"] == 0
    assert t["buckets_descriptive"]["0.90-0.95"]["n_trades"] == 20

    c = rep["criterion_test"]
    assert c["passed"] is True
    assert any("sonucu yok" in w for w in c["warnings"])

    rep2 = fa.build_report(tmp_path / "raw", out, include_second_window=True, now_ms=_ts(2026, 10, 8))
    assert rep2["windows"]["ikinci_test"]["trades"] == 1
    assert any("GECICI" in w for w in rep2["criterion_test"]["warnings"])


def test_criterion_fails_without_trades(tmp_path):
    rep = fa.build_report(tmp_path / "raw", tmp_path / "out", include_second_window=False,
                          now_ms=_ts(2026, 10, 10))
    assert rep["criterion_test"]["passed"] is False
    assert "GECMEDI" in rep["criterion_test"]["verdict"]


def test_criterion_fails_on_losses(tmp_path):
    raw = tmp_path / "raw" / "runner=longjob" / "date=2026-09-24" / "rounds.jsonl"
    rows = [_round(f"t{i}", _ts(2026, 9, 24) + i * 300_000, _side(0.90), _side(0.11)) for i in range(30)]
    _write(raw, rows)
    # %80 kazanma, 0.90 giris -> negatif
    outs = [{"round_id": f"t{i}", "outcome": "up" if i % 5 else "down"} for i in range(30)]
    _write(tmp_path / "out" / "date=2026-09-24" / "outcomes.jsonl", outs)
    rep = fa.build_report(tmp_path / "raw", tmp_path / "out", include_second_window=False,
                          now_ms=_ts(2026, 10, 10))
    t = rep["windows"]["test"]
    assert t["edge"] == pytest.approx(0.8 - 0.9)
    assert t["longest_loss_streak"] == 1
    assert rep["criterion_test"]["passed"] is False
