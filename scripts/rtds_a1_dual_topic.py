#!/usr/bin/env python3
"""Ucuncu tur izolasyon probu -- K-27'de dogrulanan A1 sekliyle (`action`
alani dahil), TEK baglantida IKI topic birden (`crypto_prices` +
`crypto_prices_chainlink`), 60 saniye, HICBIR SINIRLAMA OLMADAN (ilk N
cerceveyle kisitlanmadan) gelen HER cerceve ham kaydedilir.

Onceki kosum (probe_output/20260909T074807Z/shape_a1_action_field.json,
bkz. docs/decisions.md K-27) yalnizca `crypto_prices`'a, 20 saniyelik bir
pencerede, iki cerceve gormustu (bos cerceve + 120 noktalik gecmis dokumu)
-- pencere kisaydi ve chainlink hic denenmemisti. Bu kosum dort soruyu
aciyor:

  1. `crypto_prices_chainlink` cerceve veriyor mu? Sekli `crypto_prices`
     ile ayni mi, zaman damgasi zarfin neresinde ve hangi birimde?
  2. Ilk dokumden (initial dump, `type=subscribe`) SONRA `type=update` ile
     akan guncellemeler geliyor mu, kac saniyede bir?
  3. Chainlink'te de 120 noktalik gecmis dokumu var mi (Binance ile ayni
     pencere genisligi mi)?
  4. Bos cerceve (`raw:""`) her abonelikte tekrarliyor mu (topic basina
     bir kez mi, yoksa hic mi)?

Ayrica K-23'un sessizlik esiklerini (`silence_warn_sec`/
`silence_reconnect_sec`) tahminle degil olcumle sabitlemek icin gereken
topic-basina mesajlar-arasi gecikme dagilimini AYNI 60s pencereden
cikarir -- ayri bir baglanti gerekmez.

Baglanti/dinleme mantigi YENIDEN YAZILMADI: `scripts.rtds_cf_diagnosis
._run_connection` aynen kullanilir (sinirsiz dinleme -- `_listen_raw`,
handshake/cf_signals yakalama, gercek WS protokol cercevesi sayaci).
Bu dosya yalnizca (a) A1 sekliyle IKI topic'lik abonelik mesajini kurar,
(b) sonucu topic/tip bazinda OZETLER -- dogrulama YAPMAZ (schema/
validator bilmiyor), yalnizca goruneni sayar.

Bu prob HICBIR SEYI DUZELTMEZ, collector/'a baglanmaz -- K-27'nin "kod
duzeltmesi bundan sonra, tek PR'da (action alani + feed_ts kaynagi +
esikler)" kosuluna uygun, bu yalnizca teshis.

Kullanim:
    python -m scripts.rtds_a1_dual_topic

Cikti: probe_output/<UTC-ISO-zaman>/rtds_a1_dual_topic.json
"""

import asyncio
import json
import statistics
import sys
from pathlib import Path
from typing import Optional

from collector.endpoints import (
    RTDS_SUBSCRIPTION_TYPE,
    RTDS_SYMBOL_BINANCE,
    RTDS_SYMBOL_CHAINLINK,
    RTDS_TOPIC_BINANCE,
    RTDS_TOPIC_CHAINLINK,
)
from scripts.probe import _write_json
from scripts.rtds_cf_diagnosis import _run_connection
from scripts.rtds_raw_capture import _now_run_id

LISTEN_SECONDS = 60.0

_SYMBOL_BY_TOPIC = {
    RTDS_TOPIC_BINANCE: RTDS_SYMBOL_BINANCE,
    RTDS_TOPIC_CHAINLINK: RTDS_SYMBOL_CHAINLINK,
}


def _a1_dual_topic_message() -> str:
    """K-27'de dogrulanan sekil (`action` alani) + uretimin
    `_handle_open`'daki gibi iki topic tek mesajda (bkz.
    collector/rtds_ws.py) -- yalnizca `action` eklenmis, baska hicbir
    alan degismemis."""
    return json.dumps(
        {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": topic,
                    "type": RTDS_SUBSCRIPTION_TYPE,
                    "filters": json.dumps({"symbol": _SYMBOL_BY_TOPIC[topic]}),
                }
                for topic in (RTDS_TOPIC_BINANCE, RTDS_TOPIC_CHAINLINK)
            ],
        }
    )


def _parse_frame(raw: str) -> Optional[dict]:
    if raw == "":
        return None
    try:
        obj = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    return obj if isinstance(obj, dict) else None


def _analyze(events: list) -> dict:
    """Ham `frame_received` olaylarini topic/type/sekil bazinda ozetler.
    Dogrulama YAPMAZ -- yalnizca goruneni sayar, K-06/SCHEMA.md kurallarina
    gore hicbir satiri reddetmez/donusturmez (bu zaten `data/` akisi
    degil, bkz. modul docstring'i)."""
    frame_events = [e for e in events if e["kind"] == "frame_received"]

    empty_frame_elapsed_sec = [e["elapsed_sec"] for e in frame_events if e["raw"] == ""]
    unparsed_nonempty = []
    per_topic: dict = {}
    frame_timeline = []

    for e in frame_events:
        raw = e["raw"]
        if raw == "":
            frame_timeline.append({"elapsed_sec": e["elapsed_sec"], "kind": "empty"})
            continue

        parsed = _parse_frame(raw)
        if parsed is None:
            unparsed_nonempty.append({"elapsed_sec": e["elapsed_sec"], "raw_prefix": raw[:200]})
            frame_timeline.append({"elapsed_sec": e["elapsed_sec"], "kind": "unparsed"})
            continue

        topic = parsed.get("topic")
        envelope_type = parsed.get("type")
        envelope_timestamp = parsed.get("timestamp")
        payload = parsed.get("payload")
        is_dict_payload = isinstance(payload, dict)
        data_list = payload.get("data") if is_dict_payload else None
        has_data_dump = isinstance(data_list, list)
        has_single_value = is_dict_payload and "value" in payload

        bucket = per_topic.setdefault(
            topic,
            {
                "frame_count": 0,
                "types_seen": set(),
                "envelope_keys_seen": set(),
                "payload_keys_seen": set(),
                "envelope_timestamp_examples_ms": [],
                "initial_dump_frames": 0,
                "initial_dump_point_counts": [],
                "initial_dump_point_timestamp_range_ms": [],
                "update_frames": 0,
                "update_examples": [],
                "all_arrival_elapsed_sec": [],
                "update_arrival_elapsed_sec": [],
            },
        )
        bucket["frame_count"] += 1
        bucket["types_seen"].add(envelope_type)
        bucket["envelope_keys_seen"].update(parsed.keys())
        if is_dict_payload:
            bucket["payload_keys_seen"].update(payload.keys())
        if envelope_timestamp is not None and len(bucket["envelope_timestamp_examples_ms"]) < 5:
            bucket["envelope_timestamp_examples_ms"].append(envelope_timestamp)
        bucket["all_arrival_elapsed_sec"].append(e["elapsed_sec"])

        shape_label = "unrecognized"
        if has_data_dump:
            shape_label = f"initial_dump(n={len(data_list)})"
            bucket["initial_dump_frames"] += 1
            bucket["initial_dump_point_counts"].append(len(data_list))
            point_timestamps = [
                p.get("timestamp") for p in data_list if isinstance(p, dict) and "timestamp" in p
            ]
            if point_timestamps:
                bucket["initial_dump_point_timestamp_range_ms"].append(
                    {"min": min(point_timestamps), "max": max(point_timestamps)}
                )
        elif has_single_value:
            shape_label = "update"
            bucket["update_frames"] += 1
            bucket["update_arrival_elapsed_sec"].append(e["elapsed_sec"])
            if len(bucket["update_examples"]) < 5:
                bucket["update_examples"].append(
                    {"elapsed_sec": e["elapsed_sec"], "envelope_timestamp_ms": envelope_timestamp, "payload": payload}
                )

        frame_timeline.append(
            {"elapsed_sec": e["elapsed_sec"], "kind": f"{topic}:{envelope_type}:{shape_label}"}
        )

    per_topic_out = {}
    for topic, bucket in per_topic.items():
        update_arrivals = bucket.pop("update_arrival_elapsed_sec")
        # K-23: gap dagilimi yalnizca UPDATE cercevelerinden -- tek seferlik
        # initial dump'i katmak, K-23'un aradigi "mesajlar-arasi kalici
        # yayin kadansi"ni bir kerelik bir olayla bulandirirdi (bkz.
        # scripts/probe.py._probe_rtds_gap_distribution ile ayni ilke:
        # yalnizca payload.value tasiyan cerceveler sayilir).
        gaps = (
            [b - a for a, b in zip(update_arrivals, update_arrivals[1:])]
            if len(update_arrivals) >= 2
            else None
        )
        bucket["types_seen"] = sorted(t for t in bucket["types_seen"] if t is not None)
        bucket["envelope_keys_seen"] = sorted(bucket["envelope_keys_seen"])
        bucket["payload_keys_seen"] = sorted(bucket["payload_keys_seen"])
        bucket["all_arrival_count"] = len(bucket.pop("all_arrival_elapsed_sec"))
        bucket["update_gap_stats_sec"] = (
            {"count": len(update_arrivals), "min": min(gaps), "median": statistics.median(gaps), "max": max(gaps)}
            if gaps
            else {"count": len(update_arrivals), "min": None, "median": None, "max": None}
        )
        per_topic_out[topic or "(topic alani yok/None)"] = bucket

    return {
        "total_frame_count": len(frame_events),
        "empty_frame_count": len(empty_frame_elapsed_sec),
        "empty_frame_elapsed_sec": empty_frame_elapsed_sec,
        "unparsed_nonempty_frames": unparsed_nonempty,
        "frame_timeline": frame_timeline,
        "per_topic": per_topic_out,
    }


async def _run(connect_fn=None) -> dict:
    result = await _run_connection(
        "rtds_a1_dual_topic",
        subscribe_message=_a1_dual_topic_message(),
        listen_seconds=LISTEN_SECONDS,
        send_app_pings=True,
        connect_fn=connect_fn,
    )
    result["analysis"] = _analyze(result["events"])
    return result


def main() -> int:
    run_dir = Path("probe_output") / _now_run_id()
    result = asyncio.run(_run())
    out_path = run_dir / "rtds_a1_dual_topic.json"
    _write_json(out_path, result)

    analysis = result["analysis"]
    print(f"rtds_a1_dual_topic tamamlandi: {out_path}")
    print(
        f"  connected={result['connected']} frame_received={result['frame_received_count']}"
        f" empty_frame_count={analysis['empty_frame_count']}"
        f" (elapsed_sec={analysis['empty_frame_elapsed_sec']})"
    )
    for topic, bucket in analysis["per_topic"].items():
        print(
            f"  [{topic}] frame_count={bucket['frame_count']} types_seen={bucket['types_seen']}"
            f" initial_dump_frames={bucket['initial_dump_frames']}"
            f" initial_dump_point_counts={bucket['initial_dump_point_counts']}"
            f" initial_dump_point_timestamp_range_ms={bucket['initial_dump_point_timestamp_range_ms']}"
            f" update_frames={bucket['update_frames']}"
            f" update_gap_stats_sec={bucket['update_gap_stats_sec']}"
        )
    if analysis["unparsed_nonempty_frames"]:
        print(
            f"  UYARI: {len(analysis['unparsed_nonempty_frames'])} ayristirilamayan (bos olmayan) cerceve var"
        )
    print(f"  close=({result['close_code']!r}, {result['close_reason']!r}) error={result['error']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
