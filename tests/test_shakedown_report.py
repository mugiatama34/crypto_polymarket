"""scripts/shakedown_report.py icin testler -- salt-okunur ozet uretimi.

Gercek `writer`/`validator`'dan gecirilmis satirlar degil: build_summary
yalnizca zaten diskteki JSONL'i okuyor, bu yuzden fixture'lar dogrudan
JSON olarak yaziliyor (SCHEMA.md alan adlariyla, ama minimal).
"""

import json
from pathlib import Path

from scripts.shakedown_report import build_summary, main


def _write_jsonl(path: Path, records: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def _oracle_feed(venue: str, feed_ts_source: str) -> dict:
    return {"value": 1.0, "source": "x", "venue": venue, "feed_ts": None, "feed_ts_source": feed_ts_source}


def _setup_fixtures(tmp_path: Path) -> dict:
    raw_dir = tmp_path / "raw"
    coverage_dir = tmp_path / "coverage"
    rejected_dir = tmp_path / "rejected"

    obs_ws = {
        "offset_sec": 60,
        "offset_actual_sec": 61.5,
        "response_ts": 1500,
        "staleness_ms": 200,
        "latency_ms": None,
        "transport": "ws",
        "btc_reference": _oracle_feed("polymarket_rtds", "point"),
        "btc_oracle": _oracle_feed("chainlink", "point"),
        "status": "ok",
    }
    obs_rest = {
        "offset_sec": 60,
        "offset_actual_sec": 58.0,
        "response_ts": 1600,
        "staleness_ms": None,
        "latency_ms": 120,
        "transport": "rest",
        "btc_reference": _oracle_feed("binance", "none"),
        "btc_oracle": _oracle_feed("none", "none"),
        "status": "ok",
    }
    round_complete = {
        "job_id": "job1",
        "round_id": "r1",
        "open_ts": 1000,
        "close_ts": 2000,
        "status": "complete",
        "observations": [obs_ws, obs_rest],
        "raw": [
            {"endpoint": "rtds_binance", "payload": {"type": "subscribe"}},
            {"endpoint": "rtds_binance", "payload": {"type": "update"}},
        ],
    }
    round_missed = {
        "job_id": "job1",
        "round_id": "r2",
        "open_ts": 2000,
        "close_ts": 2500,
        "status": "missed",
        "observations": [],
        "raw": [],
    }
    _write_jsonl(raw_dir / "runner=longjob" / "date=2026-01-01" / "rounds.jsonl", [round_complete, round_missed])

    heartbeats = [
        {"job_id": "job1", "event": "job_start", "ts": 900},
        {"job_id": "job1", "event": "tick", "ts": 950},
        {"job_id": "job1", "event": "tick", "ts": 1010},
        {
            "job_id": "job1",
            "event": "job_end",
            "ts": 1100,
            "rtds_dropped_not_json": 2,
            "rtds_dropped_unknown_symbol": 1,
            "rtds_dropped_unknown_shape": 0,
        },
    ]
    _write_jsonl(coverage_dir / "runner=longjob" / "date=2026-01-01" / "heartbeat.jsonl", heartbeats)

    rejected = [
        {"errors": ["missing field: foo"], "runner_id": "longjob"},
        {"errors": ["missing field: foo", "bad type: bar"], "runner_id": "longjob"},
    ]
    _write_jsonl(rejected_dir / "runner=longjob" / "date=2026-01-01" / "rejected.jsonl", rejected)

    return {"raw_dir": raw_dir, "coverage_dir": coverage_dir, "rejected_dir": rejected_dir}


def test_build_summary_computes_expected_sections(tmp_path):
    dirs = _setup_fixtures(tmp_path)
    summary = build_summary(raw_dir=dirs["raw_dir"], coverage_dir=dirs["coverage_dir"], rejected_dir=dirs["rejected_dir"])

    assert summary["rounds_seen"] == 2
    assert summary["round_status_counts"] == {"complete": 1, "missed": 1}

    obs_dist = summary["observation_count_distribution"]
    assert obs_dist["count"] == 2
    assert obs_dist["min"] == 0
    assert obs_dist["max"] == 2
    assert obs_dist["expected"] == 24
    assert obs_dist["rounds_matching_expected"] == 0

    dev = summary["offset_actual_deviation_sec"]
    assert dev["min"] == -2.0
    assert dev["max"] == 1.5

    feed_ref = summary["feed_ts_source_distribution"]["btc_reference"]
    assert feed_ref["ws|polymarket_rtds|point"] == 1
    assert feed_ref["rest|binance|none"] == 1

    staleness = summary["staleness_ms_by_transport"]
    assert staleness["ws"]["min"] == 200
    assert staleness["ws"]["null_count"] == 0
    assert staleness["rest"]["null_count"] == 1

    latency = summary["latency_ms_rest_only"]
    assert latency["min"] == latency["max"] == 120
    assert latency["count"] == 1

    assert summary["rtds_dropped_frame_totals"] == {
        "rtds_dropped_not_json": 2,
        "rtds_dropped_unknown_symbol": 1,
        "rtds_dropped_unknown_shape": 0,
    }

    assert summary["rejected_rows"]["count"] == 2
    assert summary["rejected_rows"]["error_reason_counts"]["missing field: foo"] == 2
    assert summary["rejected_rows"]["error_reason_counts"]["bad type: bar"] == 1

    assert summary["heartbeat_gaps"]["overall_max_gap_sec"] == 0.06
    assert summary["heartbeat_gaps"]["by_job_id"]["job1"]["max_gap_sec"] == 0.06
    assert summary["heartbeat_gaps"]["by_job_id"]["job1"]["tick_count"] == 3

    assert summary["venue_distribution_rest_only"] == {"binance": 1}

    ws_frames = summary["ws_frame_type_distribution"]
    assert ws_frames["rtds_binance|subscribe"] == 1
    assert ws_frames["rtds_binance|update"] == 1

    first_complete = summary["first_complete_round_elapsed_by_job_id"]["job1"]
    assert first_complete["round_id"] == "r1"
    assert first_complete["elapsed_sec"] == 0.7


def test_main_writes_summary_files(tmp_path):
    dirs = _setup_fixtures(tmp_path)
    out_dir = tmp_path / "out"

    main(
        [
            "--raw-dir",
            str(dirs["raw_dir"]),
            "--coverage-dir",
            str(dirs["coverage_dir"]),
            "--rejected-dir",
            str(dirs["rejected_dir"]),
            "--out-dir",
            str(out_dir),
        ]
    )

    run_dirs = list(out_dir.iterdir())
    assert len(run_dirs) == 1
    summary_json = run_dirs[0] / "summary.json"
    summary_txt = run_dirs[0] / "summary.txt"
    assert summary_json.exists()
    assert summary_txt.exists()

    parsed = json.loads(summary_json.read_text(encoding="utf-8"))
    assert parsed["rounds_seen"] == 2
    assert "12. Ilk complete turun" in summary_txt.read_text(encoding="utf-8")


def test_build_summary_handles_empty_input(tmp_path):
    summary = build_summary(raw_dir=tmp_path / "raw", coverage_dir=tmp_path / "coverage", rejected_dir=tmp_path / "rejected")
    assert summary["rounds_seen"] == 0
    assert summary["round_status_counts"] == {}
    assert summary["heartbeat_gaps"]["overall_max_gap_sec"] is None
