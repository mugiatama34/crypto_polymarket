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


def test_build_summary_computes_expected_sections(tmp_path, monkeypatch):
    # K-32: fixture bir ws + bir rest gozlemi tasiyor -- bu, ws bacagi
    # acik varsayimiyla yazildi, bayragi acikca "1" yapip o varsayimi
    # koruyoruz. Kapali (varsayilan) durum icin ayri test asagida.
    monkeypatch.setenv("COLLECTOR_WS_LEG_ENABLED", "1")
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
    # K-32: fixture'daki job_end'de clob_ws_dropped_* alanlari yok --
    # eksik olan job_end'lerde 0'a duser, KeyError firlamaz.
    assert summary["clob_ws_dropped_frame_totals"] == {
        "clob_ws_dropped_not_json": 0,
        "clob_ws_dropped_unknown_event_type": 0,
        "clob_ws_dropped_unknown_shape": 0,
    }
    # K-32: fixture'daki job_end'de rounds_seen/rounds_missed/rounds_error
    # yok -- None olarak gorunur, round.status kirilimindan (madde 1) ayri.
    assert summary["round_counters_by_job_id"]["job1"] == {
        "rounds_seen": None,
        "rounds_missed": None,
        "rounds_error": None,
        "rounds_skipped_stale": None,
    }
    assert summary["round_error_exception_type_counts"] == {}

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


def test_build_summary_reports_schema_version_breakdown(tmp_path):
    """K-36: v1 -> v2 gecisinde iki surum ayni akista yan yana olabilir --
    ozet bunlari sessizce birlestirmek yerine ayri sayar."""
    raw_dir = tmp_path / "raw"
    coverage_dir = tmp_path / "coverage"
    rejected_dir = tmp_path / "rejected"

    rounds = [
        {"job_id": "job1", "round_id": "r1", "open_ts": 1000, "close_ts": 2000, "status": "complete",
         "schema_version": 1, "observations": [], "raw": []},
        {"job_id": "job1", "round_id": "r2", "open_ts": 2000, "close_ts": 2500, "status": "complete",
         "schema_version": 2, "observations": [], "raw": []},
        {"job_id": "job1", "round_id": "r3", "open_ts": 2500, "close_ts": 3000, "status": "complete",
         "schema_version": 2, "observations": [], "raw": []},
    ]
    _write_jsonl(raw_dir / "runner=longjob" / "date=2026-01-01" / "rounds.jsonl", rounds)

    summary = build_summary(raw_dir=raw_dir, coverage_dir=coverage_dir, rejected_dir=rejected_dir)

    assert summary["schema_version_counts"] == {"2": 2, "1": 1}


def test_build_summary_until_ms_excludes_records_at_or_after_boundary(tmp_path):
    """K-37: --until ust sinir HARIC ([since, until)) -- gunluk saglik
    raporunda "onceki tam UTC gunu"nu bugunun kismi verisinden ayirmak
    icin eklendi."""
    raw_dir = tmp_path / "raw"
    coverage_dir = tmp_path / "coverage"
    rejected_dir = tmp_path / "rejected"

    rounds = [
        {"job_id": "job-yesterday", "round_id": "r1", "open_ts": 1000, "close_ts": 1300, "status": "complete", "observations": [], "raw": []},
        {"job_id": "job-today", "round_id": "r2", "open_ts": 2000, "close_ts": 2300, "status": "complete", "observations": [], "raw": []},
    ]
    _write_jsonl(raw_dir / "runner=longjob" / "date=2026-01-01" / "rounds.jsonl", rounds)

    summary = build_summary(
        raw_dir=raw_dir, coverage_dir=coverage_dir, rejected_dir=rejected_dir, since_ms=500, until_ms=2000
    )

    assert summary["rounds_seen"] == 1
    assert summary["scope"] == {"job_id": None, "since_ms": 500, "until_ms": 2000}


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


def test_main_accepts_until_cli_flag(tmp_path):
    """K-37: --until argparse'a bagli, daily_health.yml'in [since, until)
    cagrisini uctan uca dogrular."""
    dirs = _setup_fixtures(tmp_path)
    out_dir = tmp_path / "out"

    main(
        [
            "--raw-dir", str(dirs["raw_dir"]),
            "--coverage-dir", str(dirs["coverage_dir"]),
            "--rejected-dir", str(dirs["rejected_dir"]),
            "--out-dir", str(out_dir),
            "--since", "1970-01-01T00:00:00.500Z",
            "--until", "1970-01-01T00:00:02.000Z",
        ]
    )

    summary_json = next(out_dir.iterdir()) / "summary.json"
    parsed = json.loads(summary_json.read_text(encoding="utf-8"))
    assert parsed["scope"]["since_ms"] == 500
    assert parsed["scope"]["until_ms"] == 2000
    assert parsed["rounds_seen"] == 1  # yalnizca round_complete (open_ts=1000), round_missed (open_ts=2000) haric


def test_build_summary_handles_empty_input(tmp_path):
    summary = build_summary(raw_dir=tmp_path / "raw", coverage_dir=tmp_path / "coverage", rejected_dir=tmp_path / "rejected")
    assert summary["rounds_seen"] == 0
    assert summary["round_status_counts"] == {}
    assert summary["heartbeat_gaps"]["overall_max_gap_sec"] is None


def test_expected_observations_defaults_to_12_when_ws_leg_disabled(tmp_path, monkeypatch):
    """K-32: bayrak ayarlanmamis/`"0"` -- ws bacagi kapali varsayilani,
    tur basina beklenen 12 gozlem (yalnizca rest)."""
    monkeypatch.delenv("COLLECTOR_WS_LEG_ENABLED", raising=False)
    dirs = _setup_fixtures(tmp_path)
    summary = build_summary(raw_dir=dirs["raw_dir"], coverage_dir=dirs["coverage_dir"], rejected_dir=dirs["rejected_dir"])
    assert summary["observation_count_distribution"]["expected"] == 12


def test_round_error_exception_type_counts_parses_runner_error_detail(tmp_path):
    """K-32: runner.py'nin round-seviyesi try/except'inin heartbeat'e
    yazdigi sabit desen -- aynı istisna coklu turda tekrarliyorsa burada
    frekans tablosu olarak gorunmeli (kullanicinin istedigi teshis)."""
    raw_dir = tmp_path / "raw"
    coverage_dir = tmp_path / "coverage"
    rejected_dir = tmp_path / "rejected"

    heartbeats = [
        {"job_id": "job1", "event": "job_start", "ts": 900},
        {
            "job_id": "job1",
            "event": "error",
            "ts": 950,
            "detail": "round isleme hatasi round=btc-updown-5m-1 exc_type=ValueError: bad book",
        },
        {
            "job_id": "job1",
            "event": "error",
            "ts": 1000,
            "detail": "round isleme hatasi round=btc-updown-5m-2 exc_type=ValueError: bad book again",
        },
        {
            "job_id": "job1",
            "event": "error",
            "ts": 1010,
            "detail": "market bulunamadi, round atlandi: btc-updown-5m-3",
        },
        {
            "job_id": "job1",
            "event": "job_end",
            "ts": 1100,
            "rounds_seen": 0,
            "rounds_missed": 1,
            "rounds_error": 2,
        },
    ]
    _write_jsonl(coverage_dir / "runner=longjob" / "date=2026-01-01" / "heartbeat.jsonl", heartbeats)

    summary = build_summary(raw_dir=raw_dir, coverage_dir=coverage_dir, rejected_dir=rejected_dir)

    assert summary["round_error_exception_type_counts"] == {"ValueError": 2}
    assert summary["round_counters_by_job_id"]["job1"] == {
        "rounds_seen": 0,
        "rounds_missed": 1,
        "rounds_error": 2,
        "rounds_skipped_stale": None,
    }


def _setup_two_job_fixtures(tmp_path: Path) -> dict:
    """K-34/K-35 rapor bulgusu: iki farkli job_id (eski koşum + yeni
    koşum) ayni gunun dosyalarinda. --job-id/--since izolasyonunu ve
    metrics_by_job_id kirilimini test etmek icin."""
    raw_dir = tmp_path / "raw"
    coverage_dir = tmp_path / "coverage"
    rejected_dir = tmp_path / "rejected"

    def _obs(offset_actual_sec: float) -> dict:
        return {
            "offset_sec": 60,
            "offset_actual_sec": offset_actual_sec,
            "response_ts": 1000,
            "staleness_ms": None,
            "latency_ms": 100,
            "transport": "rest",
            "btc_reference": _oracle_feed("coinbase", "none"),
            "btc_oracle": _oracle_feed("none", "none"),
            "status": "ok",
        }

    job_old_round = {
        "job_id": "job-old",
        "round_id": "r-old-1",
        "open_ts": 500,
        "close_ts": 800,
        "status": "complete",
        "timing_valid": True,
        "observations": [_obs(60.0)],
        "raw": [],
    }
    job_new_round_complete = {
        "job_id": "job-new",
        "round_id": "r-new-1",
        "open_ts": 5000,
        "close_ts": 5300,
        "status": "complete",
        "timing_valid": False,  # K-35: backlog artigi -- cagrilar basarili ama gec ornekelendi
        "observations": [_obs(-9000.0)],
        "raw": [],
    }
    job_new_round_partial = {
        "job_id": "job-new",
        "round_id": "r-new-2",
        "open_ts": 5300,
        "close_ts": 5600,
        "status": "partial",
        "timing_valid": False,
        "observations": [_obs(-8700.0)],
        "raw": [],
    }
    _write_jsonl(
        raw_dir / "runner=longjob" / "date=2026-01-01" / "rounds.jsonl",
        [job_old_round, job_new_round_complete, job_new_round_partial],
    )

    heartbeats = [
        {"job_id": "job-old", "event": "job_start", "ts": 100},
        {
            "job_id": "job-old",
            "event": "job_end",
            "ts": 900,
            "rounds_seen": 1,
            "rounds_missed": 0,
            "rounds_error": 0,
            "rounds_skipped_stale": 0,
        },
        {"job_id": "job-new", "event": "job_start", "ts": 4900},
        {
            "job_id": "job-new",
            "event": "job_end",
            "ts": 5700,
            "rounds_seen": 2,
            "rounds_missed": 0,
            "rounds_error": 0,
            "rounds_skipped_stale": 62,
        },
    ]
    _write_jsonl(coverage_dir / "runner=longjob" / "date=2026-01-01" / "heartbeat.jsonl", heartbeats)

    return {"raw_dir": raw_dir, "coverage_dir": coverage_dir, "rejected_dir": rejected_dir}


def test_build_summary_defaults_to_latest_job_id(tmp_path):
    """K-34/K-35 rapor bulgusu: filtre verilmezse en son job_start'a
    sahip job_id'ye izole olunur -- eski koşum sessizce karismaz."""
    dirs = _setup_two_job_fixtures(tmp_path)
    summary = build_summary(raw_dir=dirs["raw_dir"], coverage_dir=dirs["coverage_dir"], rejected_dir=dirs["rejected_dir"])

    assert summary["scope"] == {"job_id": "job-new", "since_ms": None, "until_ms": None}
    assert summary["rounds_seen"] == 2
    assert summary["round_status_counts"] == {"complete": 1, "partial": 1}
    assert set(summary["metrics_by_job_id"].keys()) == {"job-new"}
    assert summary["round_counters_by_job_id"] == {
        "job-new": {"rounds_seen": 2, "rounds_missed": 0, "rounds_error": 0, "rounds_skipped_stale": 62}
    }


def test_build_summary_job_id_filter_isolates_older_run(tmp_path):
    dirs = _setup_two_job_fixtures(tmp_path)
    summary = build_summary(
        raw_dir=dirs["raw_dir"], coverage_dir=dirs["coverage_dir"], rejected_dir=dirs["rejected_dir"], job_id="job-old"
    )

    assert summary["scope"] == {"job_id": "job-old", "since_ms": None, "until_ms": None}
    assert summary["rounds_seen"] == 1
    assert summary["round_status_counts"] == {"complete": 1}
    assert set(summary["metrics_by_job_id"].keys()) == {"job-old"}


def test_build_summary_since_filter_pools_multiple_jobs_with_breakdown(tmp_path):
    """--since ikisini de kapsayan bir esik verirse toplam havuzlanir
    ama metrics_by_job_id her koşumu ayri gosterir."""
    dirs = _setup_two_job_fixtures(tmp_path)
    summary = build_summary(
        raw_dir=dirs["raw_dir"], coverage_dir=dirs["coverage_dir"], rejected_dir=dirs["rejected_dir"], since_ms=0
    )

    assert summary["scope"] == {"job_id": None, "since_ms": 0, "until_ms": None}
    assert summary["rounds_seen"] == 3  # havuzlanmis: iki job'un toplami
    assert set(summary["metrics_by_job_id"].keys()) == {"job-old", "job-new"}
    assert summary["metrics_by_job_id"]["job-old"]["rounds_seen"] == 1
    assert summary["metrics_by_job_id"]["job-new"]["rounds_seen"] == 2


def test_build_summary_since_filter_excludes_older_job(tmp_path):
    dirs = _setup_two_job_fixtures(tmp_path)
    summary = build_summary(
        raw_dir=dirs["raw_dir"], coverage_dir=dirs["coverage_dir"], rejected_dir=dirs["rejected_dir"], since_ms=1000
    )

    assert summary["rounds_seen"] == 2
    assert set(summary["metrics_by_job_id"].keys()) == {"job-new"}


def test_complete_timing_valid_counts_flags_backlog_rounds_as_false(tmp_path):
    """K-35: status='complete' olsa bile backlog'dan gelen turlar
    timing_valid=False -- rapor bunu ayri gosterir, gizlemez."""
    dirs = _setup_two_job_fixtures(tmp_path)
    summary = build_summary(
        raw_dir=dirs["raw_dir"], coverage_dir=dirs["coverage_dir"], rejected_dir=dirs["rejected_dir"], job_id="job-new"
    )
    assert summary["complete_timing_valid_counts"] == {"False": 1}


def test_complete_timing_valid_counts_true_for_on_time_round(tmp_path):
    dirs = _setup_two_job_fixtures(tmp_path)
    summary = build_summary(
        raw_dir=dirs["raw_dir"], coverage_dir=dirs["coverage_dir"], rejected_dir=dirs["rejected_dir"], job_id="job-old"
    )
    assert summary["complete_timing_valid_counts"] == {"True": 1}


def test_main_writes_job_id_filtered_summary(tmp_path):
    dirs = _setup_two_job_fixtures(tmp_path)
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
            "--job-id",
            "job-old",
        ]
    )

    run_dirs = list(out_dir.iterdir())
    parsed = json.loads((run_dirs[0] / "summary.json").read_text(encoding="utf-8"))
    assert parsed["scope"]["job_id"] == "job-old"
    assert parsed["rounds_seen"] == 1


def test_main_since_arg_parses_iso8601_utc(tmp_path):
    dirs = _setup_two_job_fixtures(tmp_path)
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
            "--since",
            "1970-01-01T00:00:01Z",  # epoch ms 1000 -- job-old'u disarida birakir
        ]
    )

    run_dirs = list(out_dir.iterdir())
    parsed = json.loads((run_dirs[0] / "summary.json").read_text(encoding="utf-8"))
    assert parsed["scope"]["since_ms"] == 1000
    assert parsed["rounds_seen"] == 2
