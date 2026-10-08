#!/usr/bin/env python3
"""Son analiz -- docs/decisions.md K-38'deki tek kural ve tek kriter.

Kural, veri bolunmesi, kriter ve istatistik yontemi K-38'de sonuc
gorulmeden sabitlendi. Bu script onlari UYGULAR, hicbirini parametre
olarak disariya acmaz: esikler, boyut, ucret, seed, pencereler sabit
sabitlerdir. Degistirmek K-38'i degistirmek demektir (yasak).

Okur: `data/raw/runner=longjob/date=*/rounds.jsonl[.gz]` (yalnizca
`schema_version == 2`; v1 shakedown verisi iki yaridan da cikar) ve
`data/outcomes/date=*/outcomes.jsonl[.gz]`. `data/` altina HICBIR SEY
YAZMAZ.

Raporlar: kesif ve test yarisi ayri ayri (ikinci test penceresi yalnizca
`--include-second-window` ile; K-38: test yarisi gecmeden bakilmaz).
Kriterin hukmu yalnizca test yarisi icin basilir.

Kullanim:
    python -m scripts.final_analysis
    python -m scripts.final_analysis --include-second-window

Cikti: `final_report/<UTC-zaman>/report.json` + `report.txt`, ayrica stdout.

Bu bir paper olcumudur; getiri tahmini veya tavsiye degildir (K-01).
"""

import argparse
import gzip
import json
import math
import random
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

# --- K-38 sabitleri (degistirilmez) ---------------------------------------
DECISION_OFFSET_SEC = 120
TIMING_TOLERANCE_SEC = 30
TRANSPORT = "rest"
ASK_MIN = 0.80
ASK_MAX = 0.99
SIZE_SHARES = 10.0
FEE_RATE = 0.07
FEE_MULTIPLIERS = (0.0, 1.0, 2.0)
CRITERION_FEE_MULTIPLIER = 1.0
SCHEMA_VERSION_USED = 2
BOOTSTRAP_RESAMPLES = 10_000
BOOTSTRAP_SEED = 20260924
WILSON_Z = 1.96
BUCKETS = (  # (etiket, alt dahil, ust haric); son kova ust dahil
    ("0.80-0.85", 0.80, 0.85),
    ("0.85-0.90", 0.85, 0.90),
    ("0.90-0.95", 0.90, 0.95),
    ("0.95-0.99", 0.95, None),
)
WINDOWS = (
    ("kesif", date(2026, 9, 9), date(2026, 9, 23)),
    ("test", date(2026, 9, 24), date(2026, 10, 8)),
    ("ikinci_test", date(2026, 10, 9), date(2026, 10, 22)),
)
DECISION_WINDOW = "test"
_PRICE_EPS = 1e-9

SKIP_REASONS = (
    "gozlem_yok",
    "gozlem_status",
    "gozlem_zamanlama",
    "aralik_disi",
    "tie",
    "derinlik_yetersiz",
    "sonuc_yok",
    "sonuc_invalid",
)

DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_OUTCOMES_DIR = Path("data/outcomes")
DEFAULT_OUT_DIR = Path("final_report")


# --- okuma ----------------------------------------------------------------
def _open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def _jsonl_files(base_dir: Path, pattern: str) -> list[Path]:
    files = list(base_dir.glob(pattern)) + list(base_dir.glob(pattern + ".gz"))
    # Dosya tarih sirasi (date=YYYY-MM-DD dizin adi), sonra ad.
    return sorted(files, key=lambda p: (p.parent.name, p.name))


def _iter_jsonl(paths: Iterable[Path]):
    for path in paths:
        with _open_text(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)


def load_outcomes(outcomes_dir: Path) -> dict[str, str]:
    """round_id -> outcome. Tekrar varsa ilk okunan (append-only akista ilk yazilan)."""
    result: dict[str, str] = {}
    for rec in _iter_jsonl(_jsonl_files(outcomes_dir, "date=*/outcomes.jsonl")):
        rid = rec.get("round_id")
        if isinstance(rid, str) and rid not in result:
            result[rid] = rec.get("outcome")
    return result


# --- kural ----------------------------------------------------------------
@dataclass
class Trade:
    round_id: str
    close_ts: int
    side: str
    best_ask: float
    entry_price: float  # VWAP
    won: bool

    def fee(self, multiplier: float) -> float:
        p = self.entry_price
        return multiplier * SIZE_SHARES * FEE_RATE * p * (1.0 - p)

    def pnl(self, multiplier: float) -> float:
        gross = SIZE_SHARES * (1.0 - self.entry_price) if self.won else -SIZE_SHARES * self.entry_price
        return gross - self.fee(multiplier)


def _num(x) -> Optional[float]:
    if isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x):
        return float(x)
    return None


def _in_range(ask: Optional[float]) -> bool:
    return ask is not None and ASK_MIN - _PRICE_EPS <= ask <= ASK_MAX + _PRICE_EPS


def vwap_for_size(asks, size: float) -> Optional[float]:
    """asks_top5'i en iyiden yururek `size` hisseyi doldurur; dolmazsa None."""
    remaining = size
    cost = 0.0
    for level in asks or []:
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            return None
        price, qty = _num(level[0]), _num(level[1])
        if price is None or qty is None or qty <= 0:
            continue
        take = min(qty, remaining)
        cost += take * price
        remaining -= take
        if remaining <= _PRICE_EPS:
            return cost / size
    return None


def select_observation(rec: dict) -> Optional[dict]:
    for obs in rec.get("observations") or []:
        if obs.get("offset_sec") == DECISION_OFFSET_SEC and obs.get("transport") == TRANSPORT:
            return obs
    return None


def evaluate_round(rec: dict) -> tuple[str, Optional[dict]]:
    """Sonuctan bagimsiz kural adimi (K-38 madde 1-4).

    Donus: ("enter", {side, best_ask, entry_price}) veya (atlama_sebebi, None).
    """
    obs = select_observation(rec)
    if obs is None:
        return "gozlem_yok", None
    if obs.get("status") != "ok":
        return "gozlem_status", None
    actual = _num(obs.get("offset_actual_sec"))
    if actual is None or abs(actual - DECISION_OFFSET_SEC) > TIMING_TOLERANCE_SEC:
        return "gozlem_zamanlama", None

    book = obs.get("book") or {}
    asks = {side: _num((book.get(side) or {}).get("best_ask")) for side in ("up", "down")}
    in_range = [s for s in ("up", "down") if _in_range(asks[s])]
    if not in_range:
        return "aralik_disi", None
    if len(in_range) == 2:
        if abs(asks["up"] - asks["down"]) <= _PRICE_EPS:
            return "tie", None
        side = max(in_range, key=lambda s: asks[s])
    else:
        side = in_range[0]

    vwap = vwap_for_size((book.get(side) or {}).get("asks_top5"), SIZE_SHARES)
    if vwap is None:
        return "derinlik_yetersiz", None
    return "enter", {"side": side, "best_ask": asks[side], "entry_price": vwap}


def window_of(close_ts: int) -> Optional[str]:
    d = datetime.fromtimestamp(close_ts / 1000, tz=timezone.utc).date()
    for name, start, end in WINDOWS:
        if start <= d <= end:
            return name
    return None


def build_windows(raw_dir: Path, outcomes: dict[str, str]) -> tuple[dict, Counter]:
    """Pencere adi -> {"rounds": int, "skips": Counter, "trades": [Trade]}."""
    windows = {name: {"rounds": 0, "skips": Counter(), "trades": []} for name, _, _ in WINDOWS}
    excluded: Counter = Counter()
    seen: set[str] = set()
    for rec in _iter_jsonl(_jsonl_files(raw_dir, "runner=longjob/date=*/rounds.jsonl")):
        if rec.get("schema_version") != SCHEMA_VERSION_USED:
            excluded[f"schema_v{rec.get('schema_version')}"] += 1
            continue
        rid, close_ts = rec.get("round_id"), rec.get("close_ts")
        if not isinstance(rid, str) or not isinstance(close_ts, int):
            excluded["alan_eksik"] += 1
            continue
        name = window_of(close_ts)
        if name is None:
            excluded["pencere_disi"] += 1
            continue
        w = windows[name]
        if rid in seen:
            w["skips"]["tekrar"] += 1
            continue
        seen.add(rid)
        w["rounds"] += 1

        reason, entry = evaluate_round(rec)
        if entry is None:
            w["skips"][reason] += 1
            continue
        outcome = outcomes.get(rid)
        if outcome is None:
            w["skips"]["sonuc_yok"] += 1
            continue
        if outcome not in ("up", "down"):
            w["skips"]["sonuc_invalid"] += 1
            continue
        w["trades"].append(
            Trade(
                round_id=rid,
                close_ts=close_ts,
                side=entry["side"],
                best_ask=entry["best_ask"],
                entry_price=entry["entry_price"],
                won=(outcome == entry["side"]),
            )
        )
    for w in windows.values():
        w["trades"].sort(key=lambda t: (t.close_ts, t.round_id))
    return windows, excluded


# --- istatistik -----------------------------------------------------------
def wilson_interval(wins: int, n: int, z: float = WILSON_Z) -> tuple[Optional[float], Optional[float]]:
    if n == 0:
        return None, None
    phat = wins / n
    denom = 1 + z * z / n
    center = (phat + z * z / (2 * n)) / denom
    half = z * math.sqrt(phat * (1 - phat) / n + z * z / (4 * n * n)) / denom
    return center - half, center + half


def _quantile(sorted_vals: list[float], q: float) -> float:
    pos = q * (len(sorted_vals) - 1)
    lo = math.floor(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def bootstrap_mean_ci(series: dict[float, list[float]]) -> dict[float, tuple[Optional[float], Optional[float]]]:
    """Ayni yeniden ornekleme indeksleriyle her seri icin ortalamanin %95 GA'si.

    i.i.d., BOOTSTRAP_RESAMPLES tekrar, percentile (%2.5/%97.5), seed sabit.
    """
    any_series = next(iter(series.values()))
    n = len(any_series)
    if n == 0:
        return {k: (None, None) for k in series}
    rng = random.Random(BOOTSTRAP_SEED)
    population = range(n)
    means: dict[float, list[float]] = {k: [] for k in series}
    for _ in range(BOOTSTRAP_RESAMPLES):
        idx = rng.choices(population, k=n)
        for k, vals in series.items():
            means[k].append(sum(vals[i] for i in idx) / n)
    out = {}
    for k, m in means.items():
        m.sort()
        out[k] = (_quantile(m, 0.025), _quantile(m, 0.975))
    return out


def longest_loss_streak(trades: list[Trade]) -> int:
    best = cur = 0
    for t in trades:
        cur = 0 if t.won else cur + 1
        best = max(best, cur)
    return best


def max_drawdown(pnls: list[float]) -> float:
    peak = cum = 0.0
    mdd = 0.0
    for x in pnls:
        cum += x
        peak = max(peak, cum)
        mdd = max(mdd, peak - cum)
    return mdd


def _bucket_of(price: float) -> str:
    for label, lo, hi in BUCKETS:
        if price >= lo - _PRICE_EPS and (hi is None or price < hi - _PRICE_EPS):
            return label
    return BUCKETS[0][0]  # giris >= ASK_MIN garantili; buraya dusmez


def _win_entry_edge(trades: list[Trade]) -> dict:
    n = len(trades)
    wins = sum(t.won for t in trades)
    if n == 0:
        return {"n_trades": 0, "wins": 0, "win_rate": None, "mean_entry_price": None,
                "edge": None, "edge_ci95_wilson": [None, None]}
    win_rate = wins / n
    mean_entry = sum(t.entry_price for t in trades) / n
    lo, hi = wilson_interval(wins, n)
    return {
        "n_trades": n,
        "wins": wins,
        "win_rate": win_rate,
        "mean_entry_price": mean_entry,
        "edge": win_rate - mean_entry,
        "edge_ci95_wilson": [lo - mean_entry, hi - mean_entry],
    }


def summarize_window(w: dict) -> dict:
    trades: list[Trade] = w["trades"]
    skips: Counter = w["skips"]
    pnl_series = {m: [t.pnl(m) for t in trades] for m in FEE_MULTIPLIERS}
    cis = bootstrap_mean_ci(pnl_series)
    fee_sens = {}
    for m in FEE_MULTIPLIERS:
        vals = pnl_series[m]
        fee_sens[f"{m:g}x"] = {
            "mean_net_pnl": (sum(vals) / len(vals)) if vals else None,
            "mean_net_pnl_ci95_bootstrap": list(cis[m]),
            "total_pnl": sum(vals),
        }
    base = pnl_series[CRITERION_FEE_MULTIPLIER]

    buckets = {}
    for label, _, _ in BUCKETS:
        bt = [t for t in trades if _bucket_of(t.entry_price) == label]
        b = _win_entry_edge(bt)
        b["mean_net_pnl_1x"] = (sum(t.pnl(1.0) for t in bt) / len(bt)) if bt else None
        buckets[label] = b

    return {
        "rounds": w["rounds"],
        "trades": len(trades),
        "skipped": {r: skips.get(r, 0) for r in SKIP_REASONS + ("tekrar",)},
        "skipped_total": sum(skips.values()) - skips.get("tekrar", 0),
        **{k: v for k, v in _win_entry_edge(trades).items() if k != "n_trades"},
        "fee_sensitivity": fee_sens,
        "total_pnl_1x": sum(base),
        "longest_loss_streak": longest_loss_streak(trades),
        "max_drawdown_1x": max_drawdown(base),
        "side_counts": dict(Counter(t.side for t in trades)),
        "buckets_descriptive": buckets,
    }


def criterion(summary: dict) -> dict:
    lo = summary["fee_sensitivity"][f"{CRITERION_FEE_MULTIPLIER:g}x"]["mean_net_pnl_ci95_bootstrap"][0]
    passed = summary["trades"] > 0 and lo is not None and lo > 0
    return {
        "rule": "test yarisi, 1x ucret, islem basina ortalama net PnL %95 GA alt siniri > 0",
        "ci_lower": lo,
        "passed": passed,
        "verdict": (
            "GECTI -- K-38 geregi ikinci test penceresine (9-22 Ekim) aynen uygulanir; "
            "o da gecmeden 'kanitlandi' sayilmaz"
            if passed
            else "GECMEDI -- strateji edge gostermedi; K-38 geregi longjob cron'u kapatilir, proje biter"
        ),
    }


def build_report(raw_dir: Path, outcomes_dir: Path, *, include_second_window: bool, now_ms: int) -> dict:
    outcomes = load_outcomes(outcomes_dir)
    windows, excluded = build_windows(raw_dir, outcomes)
    names = ["kesif", "test"] + (["ikinci_test"] if include_second_window else [])
    report = {
        "k38": {
            "decision_offset_sec": DECISION_OFFSET_SEC,
            "timing_tolerance_sec": TIMING_TOLERANCE_SEC,
            "ask_range_inclusive": [ASK_MIN, ASK_MAX],
            "size_shares": SIZE_SHARES,
            "entry_price_basis": "vwap_asks_top5",
            "fee_formula": "multiplier * shares * 0.07 * p * (1-p), girişte bir kez",
            "bootstrap": {"resamples": BOOTSTRAP_RESAMPLES, "seed": BOOTSTRAP_SEED, "method": "iid_percentile"},
            "edge_ci_note": "Wilson %95 (kazanma orani) - ortalama giris fiyati; giris fiyati ortalamasi sabit kabul edildi",
        },
        "generated_at": datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat(),
        "excluded_rows": dict(excluded),
        "outcomes_loaded": len(outcomes),
        "windows": {n: summarize_window(windows[n]) for n in names},
    }
    test_end = datetime(2026, 10, 9, tzinfo=timezone.utc).timestamp() * 1000
    crit = criterion(report["windows"][DECISION_WINDOW])
    warnings = []
    if now_ms < test_end:
        warnings.append("test penceresi henuz kapanmadi (8 Ekim 23:59:59 UTC) -- hukum GECICI, karar icin kullanilamaz")
    missing = report["windows"][DECISION_WINDOW]["skipped"]["sonuc_yok"]
    if missing:
        warnings.append(f"test yarisinda {missing} islem adayi turun sonucu yok -- fetch_outcomes'i tekrar calistir")
    crit["warnings"] = warnings
    report["criterion_test"] = crit
    return report


# --- cikti ----------------------------------------------------------------
def _f(x, nd=4):
    return "—" if x is None else f"{x:.{nd}f}"


def format_text(report: dict) -> str:
    L = ["Son analiz (K-38) -- paper olcum, tavsiye degildir", ""]
    L.append(f"uretim: {report['generated_at']}  |  yuklenen sonuc: {report['outcomes_loaded']}")
    L.append(f"analiz disi satirlar: {report['excluded_rows']}")
    for name, s in report["windows"].items():
        L += ["", f"=== {name} " + ("(KARAR)" if name == DECISION_WINDOW else "(aciklayici)") + " ==="]
        L.append(f"tur: {s['rounds']}  islem: {s['trades']}  atlanan: {s['skipped_total']}")
        L.append("  atlama sebepleri: " + ", ".join(f"{k}={v}" for k, v in s["skipped"].items()))
        L.append(f"  taraf: {s['side_counts']}")
        L.append(
            f"kazanma orani {_f(s['win_rate'])}  |  ort. giris (VWAP) {_f(s['mean_entry_price'])}  |  "
            f"edge {_f(s['edge'])}  Wilson %95 [{_f(s['edge_ci95_wilson'][0])}, {_f(s['edge_ci95_wilson'][1])}]"
        )
        for k, fs in s["fee_sensitivity"].items():
            lo, hi = fs["mean_net_pnl_ci95_bootstrap"]
            L.append(
                f"  ucret {k}: islem basi net PnL {_f(fs['mean_net_pnl'])} $  %95 GA [{_f(lo)}, {_f(hi)}]  "
                f"toplam {_f(fs['total_pnl'], 2)} $"
            )
        L.append(
            f"toplam PnL (1x) {_f(s['total_pnl_1x'], 2)} $  |  en uzun kayip serisi {s['longest_loss_streak']}  |  "
            f"maks. drawdown (1x) {_f(s['max_drawdown_1x'], 2)} $"
        )
        L.append("  kovalar (aciklayici, kural degistirmek icin kullanilamaz):")
        for label, b in s["buckets_descriptive"].items():
            ci = b["edge_ci95_wilson"]
            L.append(
                f"    {label}: n={b['n_trades']}  kazanma {_f(b['win_rate'])} / giris {_f(b['mean_entry_price'])}  "
                f"edge {_f(b['edge'])} [{_f(ci[0])}, {_f(ci[1])}]  net PnL/islem {_f(b['mean_net_pnl_1x'])}"
            )
    c = report["criterion_test"]
    L += ["", "=== KRITER (yalnizca test yarisi) ===", c["rule"], f"GA alt siniri: {_f(c['ci_lower'])}", c["verdict"]]
    for w in c["warnings"]:
        L.append(f"UYARI: {w}")
    return "\n".join(L)


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    p.add_argument("--outcomes-dir", type=Path, default=DEFAULT_OUTCOMES_DIR)
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    p.add_argument("--include-second-window", action="store_true",
                   help="9-22 Ekim penceresini de raporla (K-38: yalnizca test yarisi gectiyse)")
    args = p.parse_args(argv)

    now_ms = int(time.time() * 1000)
    report = build_report(args.raw_dir, args.outcomes_dir,
                          include_second_window=args.include_second_window, now_ms=now_ms)
    text = format_text(report)
    stamp = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = args.out_dir / stamp
    out.mkdir(parents=True, exist_ok=True)
    (out / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "report.txt").write_text(text + "\n", encoding="utf-8")
    print(text)
    print(f"\nyazildi: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
