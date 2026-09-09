#!/usr/bin/env python3
"""Kisa gercek kosum (shakedown) icin salt-okunur saglik raporu.

Bu script:

- HICBIR SEY YAZMAZ `data/` altina -- yalnizca `data/raw/`,
  `data/coverage/`, `data/rejected/` icindeki JSONL'i okur.
- METRIK DEGIL. Edge, kazanma orani, guven araligi HESAPLAMAZ (CLAUDE.md
  "su anki faz" kisiti -- bkz. docs/decisions.md K-15). Yalnizca kapsama/
  saglik/gecikme sayimlari uretir: kac tur gorulmus, offset sapmasi ne
  kadar, hangi feed'ler ne siklikta dolu, ne kadar cerceve dusmus vb.

Kullanim:
    python -m scripts.shakedown_report

Cikti: `shakedown_output/<UTC-ISO-zaman>/summary.json` (yapilandirilmis)
+ `summary.txt` (insan-okunur, ayni dizin) -- ayrica stdout'a basilir.
`probe_output/` ile KARISTIRILMAZ: o ham prob ciktisidir, bu ise
`data/`'daki gercek gozlem akisinin bir ozetidir, kendisi SCHEMA.md
kapsaminda degildir (bkz. gorev tanimi).
"""

import argparse
import json
import os
import re
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_COVERAGE_DIR = Path("data/coverage")
DEFAULT_REJECTED_DIR = Path("data/rejected")
DEFAULT_OUT_DIR = Path("shakedown_output")

# K-32: koleksiyoncuyla ayni bayrak -- ws bacagi kapaliyken tur basina 12
# gozlem beklenir (yalnizca rest), aciksa 24 (12 offset x 2 transport,
# SCHEMA.md bolum 3).
WS_LEG_ENABLED_ENV_VAR = "COLLECTOR_WS_LEG_ENABLED"
RTDS_RAW_ENDPOINTS = ("rtds_binance", "rtds_chainlink")


def _expected_observations_per_round() -> int:
    return 24 if os.environ.get(WS_LEG_ENABLED_ENV_VAR) == "1" else 12

# K-32 PR'i: runner.py'nin round-seviyesi try/except'inin heartbeat'e
# yazdigi sabit desen -- bkz. collector/runner.py `run()`.
ROUND_ERROR_DETAIL_RE = re.compile(r"^round isleme hatasi round=\S+ exc_type=(\w+):")


def _iter_jsonl(base_dir: Path, filename: str):
    for path in sorted(base_dir.glob(f"runner=*/date=*/{filename}")):
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)


def _dist(values: list) -> dict:
    non_null = [v for v in values if v is not None]
    out = {"count": len(values), "null_count": len(values) - len(non_null)}
    if non_null:
        out["min"] = min(non_null)
        out["median"] = statistics.median(non_null)
        out["max"] = max(non_null)
    else:
        out["min"] = out["median"] = out["max"] = None
    return out


def _counter_to_dict(counter: Counter) -> dict:
    return {("|".join(map(str, k)) if isinstance(k, tuple) else str(k)): v for k, v in counter.most_common()}


def _round_finished_at_ms(round_record: dict) -> Optional[int]:
    response_ts_values = [o.get("response_ts") for o in round_record.get("observations", []) if o.get("response_ts") is not None]
    if response_ts_values:
        return max(response_ts_values)
    return round_record.get("close_ts")


def build_summary(*, raw_dir: Path, coverage_dir: Path, rejected_dir: Path) -> dict:
    rounds = list(_iter_jsonl(raw_dir, "rounds.jsonl"))
    heartbeats = list(_iter_jsonl(coverage_dir, "heartbeat.jsonl"))
    rejected = list(_iter_jsonl(rejected_dir, "rejected.jsonl"))

    # 1. tur sayisi, status kirilimi
    round_status_counts = Counter(r.get("status") for r in rounds)

    # 2. tur basina gozlem sayisi dagilimi
    obs_counts = [len(r.get("observations", [])) for r in rounds]
    expected_observations_per_round = _expected_observations_per_round()
    observation_count_dist = _dist(obs_counts)
    observation_count_dist["expected"] = expected_observations_per_round
    observation_count_dist["rounds_matching_expected"] = sum(
        1 for c in obs_counts if c == expected_observations_per_round
    )

    # 3. offset_actual_sec sapma dagilimi (isaretli)
    offset_deviations = []
    # 4. feed_ts_source dagilimi (transport, venue kirilimiyla) -- reference/oracle ayri
    feed_ts_source_counts = {"btc_reference": Counter(), "btc_oracle": Counter()}
    # 5. staleness_ms dagilimi, transport ayri
    staleness_by_transport = defaultdict(list)
    # 6. latency_ms dagilimi, yalnizca rest
    latency_rest = []
    # 10. venue dagilimi, yalnizca rest
    venue_rest_counts = Counter()

    for r in rounds:
        for o in r.get("observations", []):
            offset_sec = o.get("offset_sec")
            offset_actual_sec = o.get("offset_actual_sec")
            if offset_sec is not None and offset_actual_sec is not None:
                offset_deviations.append(offset_actual_sec - offset_sec)

            transport = o.get("transport")
            staleness_by_transport[transport].append(o.get("staleness_ms"))
            if transport == "rest":
                latency_rest.append(o.get("latency_ms"))

            for field_name, key in (("btc_reference", "btc_reference"), ("btc_oracle", "btc_oracle")):
                feed = o.get(key) or {}
                feed_ts_source_counts[field_name][(transport, feed.get("venue"), feed.get("feed_ts_source"))] += 1

            if transport == "rest":
                venue_rest_counts[(o.get("btc_reference") or {}).get("venue")] += 1

    offset_deviation_dist = _dist(offset_deviations)
    staleness_dist_by_transport = {t: _dist(v) for t, v in staleness_by_transport.items()}
    latency_rest_dist = _dist(latency_rest)

    # 7. dusen cerceve sayaclari (yalnizca job_end'de bulunan opsiyonel alanlar)
    rtds_dropped_frame_totals = {
        "rtds_dropped_not_json": 0,
        "rtds_dropped_unknown_symbol": 0,
        "rtds_dropped_unknown_shape": 0,
    }
    clob_ws_dropped_frame_totals = {
        "clob_ws_dropped_not_json": 0,
        "clob_ws_dropped_unknown_event_type": 0,
        "clob_ws_dropped_unknown_shape": 0,
    }
    # K-32 PR'i: job_end'deki rounds_seen/rounds_missed/rounds_error --
    # rounds.jsonl'a hic yazilmamis (kesif basarisiz ya da beklenmeyen
    # istisna) turlar icin, round.status kirilimindan (madde 1) AYRI.
    round_counters_by_job_id = {}
    # K-32 PR'i: round isleme hatasinda heartbeat'e yazilan istisna tipi
    # frekansi -- aynı hata coklu turda tekrarliyorsa burada gorunur.
    round_error_exception_type_counts = Counter()
    job_starts_by_job_id = {}
    for h in heartbeats:
        if h.get("event") == "job_end":
            for key in rtds_dropped_frame_totals:
                rtds_dropped_frame_totals[key] += h.get(key) or 0
            for key in clob_ws_dropped_frame_totals:
                clob_ws_dropped_frame_totals[key] += h.get(key) or 0
            job_id = h.get("job_id")
            round_counters_by_job_id[job_id] = {
                "rounds_seen": h.get("rounds_seen"),
                "rounds_missed": h.get("rounds_missed"),
                "rounds_error": h.get("rounds_error"),
            }
        if h.get("event") == "error":
            match = ROUND_ERROR_DETAIL_RE.match(h.get("detail") or "")
            if match:
                round_error_exception_type_counts[match.group(1)] += 1
        if h.get("event") == "job_start":
            job_id = h.get("job_id")
            if job_id is not None and job_id not in job_starts_by_job_id:
                job_starts_by_job_id[job_id] = h.get("ts")

    # 8. dogrulamayi gecemeyen satir sayisi + sebep frekansi
    rejected_error_counts = Counter()
    for entry in rejected:
        for err in entry.get("errors") or []:
            rejected_error_counts[err] += 1

    # 9. heartbeat bosluklari: job basina + genel en uzun ardisik bosluk (job_start/tick arasi)
    by_job = defaultdict(list)
    for h in heartbeats:
        if h.get("event") in ("job_start", "tick"):
            by_job[h.get("job_id")].append(h.get("ts"))
    heartbeat_gaps_by_job = {}
    overall_max_gap_sec = None
    for job_id, ts_list in by_job.items():
        ts_sorted = sorted(t for t in ts_list if t is not None)
        gaps = [(b - a) / 1000.0 for a, b in zip(ts_sorted, ts_sorted[1:])]
        max_gap = max(gaps) if gaps else None
        heartbeat_gaps_by_job[job_id] = {"tick_count": len(ts_sorted), "max_gap_sec": max_gap}
        if max_gap is not None and (overall_max_gap_sec is None or max_gap > overall_max_gap_sec):
            overall_max_gap_sec = max_gap

    # 11. WS'ten gelen cerceve type dagilimi -- K-28b'nin cevabi
    ws_frame_type_counts = Counter()
    for r in rounds:
        for raw_entry in r.get("raw", []):
            endpoint = raw_entry.get("endpoint")
            if endpoint not in RTDS_RAW_ENDPOINTS:
                continue
            payload = raw_entry.get("payload") or {}
            ws_frame_type_counts[(endpoint, payload.get("type"))] += 1

    # 12. ilk complete turun job_start'tan elapsed suresi
    rounds_by_job = defaultdict(list)
    for r in rounds:
        rounds_by_job[r.get("job_id")].append(r)
    first_complete_round_elapsed = {}
    for job_id, job_rounds in rounds_by_job.items():
        job_start_ts = job_starts_by_job_id.get(job_id)
        ordered = sorted(job_rounds, key=lambda r: (r.get("open_ts") is None, r.get("open_ts")))
        first_complete = next((r for r in ordered if r.get("status") == "complete"), None)
        if job_start_ts is None or first_complete is None:
            first_complete_round_elapsed[job_id] = None
            continue
        finished_at_ms = _round_finished_at_ms(first_complete)
        elapsed_sec = None if finished_at_ms is None else (finished_at_ms - job_start_ts) / 1000.0
        first_complete_round_elapsed[job_id] = {
            "round_id": first_complete.get("round_id"),
            "job_start_ts": job_start_ts,
            "elapsed_sec": elapsed_sec,
        }

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "rounds_seen": len(rounds),
        "round_status_counts": dict(round_status_counts),
        "observation_count_distribution": observation_count_dist,
        "offset_actual_deviation_sec": offset_deviation_dist,
        "feed_ts_source_distribution": {
            field_name: _counter_to_dict(counter) for field_name, counter in feed_ts_source_counts.items()
        },
        "staleness_ms_by_transport": staleness_dist_by_transport,
        "latency_ms_rest_only": latency_rest_dist,
        "rtds_dropped_frame_totals": rtds_dropped_frame_totals,
        "clob_ws_dropped_frame_totals": clob_ws_dropped_frame_totals,
        "rejected_rows": {"count": len(rejected), "error_reason_counts": _counter_to_dict(rejected_error_counts)},
        "heartbeat_gaps": {"overall_max_gap_sec": overall_max_gap_sec, "by_job_id": heartbeat_gaps_by_job},
        "venue_distribution_rest_only": _counter_to_dict(venue_rest_counts),
        "ws_frame_type_distribution": _counter_to_dict(ws_frame_type_counts),
        "first_complete_round_elapsed_by_job_id": first_complete_round_elapsed,
        "round_counters_by_job_id": round_counters_by_job_id,
        "round_error_exception_type_counts": _counter_to_dict(round_error_exception_type_counts),
    }


def _format_dist(d: dict) -> str:
    if d.get("min") is None:
        return f"count={d['count']} null={d['null_count']} (veri yok)"
    return f"count={d['count']} null={d['null_count']} min={d['min']} medyan={d['median']} maks={d['max']}"


def render_text(summary: dict) -> str:
    lines = [
        f"Shakedown saglik raporu -- {summary['generated_at_utc']}",
        "",
        f"1. Tur sayisi: {summary['rounds_seen']} -- durum kirilimi: {summary['round_status_counts']}",
        f"2. Tur basina gozlem sayisi: {_format_dist(summary['observation_count_distribution'])} "
        f"(beklenen {summary['observation_count_distribution']['expected']}, "
        f"tam eslesen {summary['observation_count_distribution']['rounds_matching_expected']} tur)",
        f"3. offset_actual_sec sapmasi (sn): {_format_dist(summary['offset_actual_deviation_sec'])}",
        f"4. feed_ts_source dagilimi (btc_reference): {summary['feed_ts_source_distribution']['btc_reference']}",
        f"   feed_ts_source dagilimi (btc_oracle): {summary['feed_ts_source_distribution']['btc_oracle']}",
        "5. staleness_ms (transport basina):",
    ]
    for transport, dist in summary["staleness_ms_by_transport"].items():
        lines.append(f"   {transport}: {_format_dist(dist)}")
    lines.append(f"6. latency_ms (yalnizca rest): {_format_dist(summary['latency_ms_rest_only'])}")
    lines.append(f"7. Dusen RTDS cerceve sayaclari: {summary['rtds_dropped_frame_totals']}")
    lines.append(
        f"8. Reddedilen satir sayisi: {summary['rejected_rows']['count']} -- "
        f"sebepler: {summary['rejected_rows']['error_reason_counts']}"
    )
    lines.append(
        f"9. Heartbeat bosluklari -- genel en uzun: {summary['heartbeat_gaps']['overall_max_gap_sec']} sn, "
        f"job basina: {summary['heartbeat_gaps']['by_job_id']}"
    )
    lines.append(f"10. venue dagilimi (yalnizca rest): {summary['venue_distribution_rest_only']}")
    lines.append(f"11. WS cerceve type dagilimi (K-28b): {summary['ws_frame_type_distribution']}")
    lines.append(f"12. Ilk complete turun job_start'tan elapsed suresi: {summary['first_complete_round_elapsed_by_job_id']}")
    lines.append(f"13. Dusen CLOB WS cerceve sayaclari: {summary['clob_ws_dropped_frame_totals']}")
    lines.append(
        "14. Runner-seviyesi tur sayaclari (job_end, rounds_seen/rounds_missed/rounds_error): "
        f"{summary['round_counters_by_job_id']}"
    )
    lines.append(
        "15. Round isleme istisna tipi dagilimi (rounds_error'a katkida bulunan): "
        f"{summary['round_error_exception_type_counts']}"
    )
    return "\n".join(lines) + "\n"


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--coverage-dir", type=Path, default=DEFAULT_COVERAGE_DIR)
    parser.add_argument("--rejected-dir", type=Path, default=DEFAULT_REJECTED_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args(argv)

    summary = build_summary(raw_dir=args.raw_dir, coverage_dir=args.coverage_dir, rejected_dir=args.rejected_dir)
    text = render_text(summary)

    out_dir = args.out_dir / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    (out_dir / "summary.txt").write_text(text, encoding="utf-8")

    print(text)
    print(f"yazildi: {out_dir}", file=sys.stderr)


if __name__ == "__main__":
    main()
