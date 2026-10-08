import json
from pathlib import Path

import httpx

from scripts import fetch_outcomes as fo


def _event(closed=True, prices=("1", "0"), outcomes=("Up", "Down"), closed_time="2026-10-07 00:06:12+00"):
    market = {
        "slug": "x",
        "closed": closed,
        "outcomes": json.dumps(list(outcomes)),
        "outcomePrices": json.dumps(list(prices)),
        "endDate": "2026-10-07T00:05:00Z",
    }
    if closed_time is not None:
        market["closedTime"] = closed_time
    return [{"slug": "x", "closed": closed, "markets": [market]}]


def _write_round(raw_dir: Path, date: str, round_id: str, close_ts: int):
    path = raw_dir / "runner=longjob" / f"date={date}" / "rounds.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps({"round_id": round_id, "close_ts": close_ts}) + "\n")


def test_parse_up_down_invalid():
    st, rec = fo.parse_outcome(_event(prices=("1", "0")), "r1")
    assert st == "written" and rec["outcome"] == "up"
    st, rec = fo.parse_outcome(_event(prices=("0", "1")), "r1")
    assert rec["outcome"] == "down"
    # outcomes sirasi ters gelse de etiketle eslesir
    st, rec = fo.parse_outcome(_event(prices=("1", "0"), outcomes=("Down", "Up")), "r1")
    assert rec["outcome"] == "down"
    st, rec = fo.parse_outcome(_event(prices=("0.5", "0.5")), "r1")
    assert rec["outcome"] == "invalid"
    assert rec["open_price"] is None and rec["close_price"] is None
    assert rec["raw"]["resolved_ts_field"] == "closedTime"


def test_parse_not_written_when_unresolved():
    assert fo.parse_outcome(_event(closed=False), "r")[0] == "pending_not_closed"
    assert fo.parse_outcome(_event(prices=("0.97", "0.03")), "r")[0] == "pending_not_final"
    assert fo.parse_outcome([], "r")[0] == "pending_not_found"
    assert fo.parse_outcome(_event(outcomes=("Yes", "No")), "r")[0] == "parse_error"


def test_resolved_ts_falls_back_and_records_field():
    st, rec = fo.parse_outcome(_event(closed_time=None), "r")
    assert rec["raw"]["resolved_ts_field"] == "endDate"
    assert rec["resolved_ts"] == 1791331500000


class _FakeFetcher:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def fetch_event(self, slug):
        self.calls.append(slug)
        return self.responses.get(slug)


def test_run_appends_and_is_idempotent(tmp_path):
    raw, out, rej = tmp_path / "raw", tmp_path / "out", tmp_path / "rej"
    now = 1_800_000_000_000
    _write_round(raw, "2026-10-01", "a", 1_759_300_000_000)
    _write_round(raw, "2026-10-01", "a", 1_759_300_000_000)  # tekrar
    _write_round(raw, "2026-10-01", "b", 1_759_300_300_000)
    _write_round(raw, "2026-10-01", "c", 1_759_300_600_000)
    _write_round(raw, "2026-10-01", "fresh", now - 60_000)  # cok yeni, aday degil
    fetcher = _FakeFetcher({"a": _event(), "b": _event(closed=False)})

    c1 = fo.run(raw_dir=raw, outcomes_dir=out, rejected_dir=rej, fetcher=fetcher, now_ms=now)
    assert c1["candidates"] == 3
    assert c1["written"] == 1 and c1["pending_not_closed"] == 1 and c1["fetch_error"] == 1
    files = list(out.glob("date=*/outcomes.jsonl"))
    assert len(files) == 1 and files[0].parent.name == "date=2025-10-01"

    fetcher2 = _FakeFetcher({"b": _event(prices=("0", "1"))})
    c2 = fo.run(raw_dir=raw, outcomes_dir=out, rejected_dir=rej, fetcher=fetcher2, now_ms=now)
    assert c2["already_written"] == 1
    assert fetcher2.calls == ["b", "c"]
    lines = [json.loads(l) for f in out.glob("date=*/outcomes.jsonl") for l in f.read_text().splitlines()]
    assert sorted((r["round_id"], r["outcome"]) for r in lines) == [("a", "up"), ("b", "down")]


def test_invalid_record_goes_to_rejected(tmp_path, monkeypatch):
    raw, out, rej = tmp_path / "raw", tmp_path / "out", tmp_path / "rej"
    _write_round(raw, "2026-10-01", "a", 1_759_300_000_000)
    monkeypatch.setattr(fo, "OUTCOME_SCHEMA_VERSION", 99)
    c = fo.run(raw_dir=raw, outcomes_dir=out, rejected_dir=rej, fetcher=_FakeFetcher({"a": _event()}), now_ms=1_800_000_000_000)
    assert c["rejected"] == 1
    assert not list(out.glob("**/outcomes.jsonl"))
    assert list(rej.glob("runner=reconciler/date=*/rejected.jsonl"))


def test_rate_limit_and_retry():
    t = [0.0]
    sleeps = []

    def sleep(s):
        sleeps.append(s)
        t[0] += s

    statuses = iter([429, 200, 200])

    def handler(request):
        return httpx.Response(next(statuses), json=[])

    client = httpx.Client(transport=httpx.MockTransport(handler))
    f = fo.RateLimitedFetcher(client, min_interval_sec=0.4, sleep=sleep, clock=lambda: t[0])
    assert f.fetch_event("a") == []
    assert f.fetch_event("b") == []
    assert f.requests == 3
    assert sleeps[0] == 2.0  # 429 sonrasi geri cekilme
    # ikinci cagri, son istekten hemen sonra -> 0.4 sn bekler
    assert sleeps[-1] == 0.4


def test_non_retryable_status_returns_none():
    client = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(404)))
    f = fo.RateLimitedFetcher(client, min_interval_sec=0, sleep=lambda s: None)
    assert f.fetch_event("a") is None
    assert f.requests == 1
