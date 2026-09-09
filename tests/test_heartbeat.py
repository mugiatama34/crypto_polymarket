import json

from collector.heartbeat import HeartbeatWriter


def _clock(values):
    it = iter(values)
    return lambda: next(it)


def test_job_start_writes_record_and_sets_last_tick(tmp_path):
    now = _clock([1717000000000])
    hb = HeartbeatWriter(runner_id="longjob", job_id="job-1", now_ms_fn=now, base_dir=tmp_path)
    path = hb.job_start(detail="exchange=binance")

    record = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    assert record["event"] == "job_start"
    assert record["runner_id"] == "longjob"
    assert record["detail"] == "exchange=binance"
    assert hb.last_tick_ms == 1717000000000


def test_due_for_tick_true_after_interval(tmp_path):
    now = _clock([1000, 1000 + 60_000])
    hb = HeartbeatWriter(runner_id="longjob", job_id="job-1", now_ms_fn=now, base_dir=tmp_path)
    hb.job_start()
    assert hb.due_for_tick() is True


def test_due_for_tick_false_before_interval(tmp_path):
    now = _clock([1000, 1000 + 30_000])
    hb = HeartbeatWriter(runner_id="longjob", job_id="job-1", now_ms_fn=now, base_dir=tmp_path)
    hb.job_start()
    assert hb.due_for_tick() is False


def test_tick_resets_last_tick_ms(tmp_path):
    now = _clock([1000, 61000, 61000])
    hb = HeartbeatWriter(runner_id="longjob", job_id="job-1", now_ms_fn=now, base_dir=tmp_path)
    hb.job_start()
    hb.tick()
    assert hb.last_tick_ms == 61000


def test_error_event_does_not_reset_tick_clock(tmp_path):
    now = _clock([1000, 5000, 5000])
    hb = HeartbeatWriter(runner_id="longjob", job_id="job-1", now_ms_fn=now, base_dir=tmp_path)
    hb.job_start()
    hb.error("ws disconnect 4200ms")
    assert hb.last_tick_ms == 1000


def test_job_end_carries_rounds_seen_and_missed(tmp_path):
    now = _clock([1000, 2000])
    hb = HeartbeatWriter(runner_id="longjob", job_id="job-1", now_ms_fn=now, base_dir=tmp_path)
    hb.job_start()
    path = hb.job_end(
        rounds_seen=10,
        rounds_missed=2,
        discovery_slug_hits=9,
        discovery_listing_hits=1,
        rtds_dropped_not_json=0,
        rtds_dropped_unknown_symbol=0,
        rtds_dropped_unknown_shape=0,
        detail="clean shutdown",
    )
    record = json.loads(path.read_text(encoding="utf-8").splitlines()[-1])
    assert record["rounds_seen"] == 10
    assert record["rounds_missed"] == 2


def test_job_end_writes_discovery_counters(tmp_path):
    now = _clock([1000, 2000])
    hb = HeartbeatWriter(runner_id="longjob", job_id="job-1", now_ms_fn=now, base_dir=tmp_path)
    hb.job_start()
    path = hb.job_end(
        rounds_seen=5,
        rounds_missed=0,
        discovery_slug_hits=4,
        discovery_listing_hits=1,
        rtds_dropped_not_json=0,
        rtds_dropped_unknown_symbol=0,
        rtds_dropped_unknown_shape=0,
    )
    record = json.loads(path.read_text(encoding="utf-8").splitlines()[-1])
    assert record["discovery_slug_hits"] == 4
    assert record["discovery_listing_hits"] == 1


def test_job_end_writes_rtds_dropped_counters(tmp_path):
    """K-25/K-29: sessizce dusen RTDS cerceveleri icin sayac gorunurlugu."""
    now = _clock([1000, 2000])
    hb = HeartbeatWriter(runner_id="longjob", job_id="job-1", now_ms_fn=now, base_dir=tmp_path)
    hb.job_start()
    path = hb.job_end(
        rounds_seen=5,
        rounds_missed=0,
        discovery_slug_hits=4,
        discovery_listing_hits=1,
        rtds_dropped_not_json=3,
        rtds_dropped_unknown_symbol=1,
        rtds_dropped_unknown_shape=2,
    )
    record = json.loads(path.read_text(encoding="utf-8").splitlines()[-1])
    assert record["rtds_dropped_not_json"] == 3
    assert record["rtds_dropped_unknown_symbol"] == 1
    assert record["rtds_dropped_unknown_shape"] == 2


def test_non_job_end_events_do_not_include_discovery_keys(tmp_path):
    now = _clock([1000, 2000, 3000])
    hb = HeartbeatWriter(runner_id="longjob", job_id="job-1", now_ms_fn=now, base_dir=tmp_path)
    hb.job_start()
    hb.tick()
    hb.error("boom")

    lines = list(tmp_path.rglob("heartbeat.jsonl"))[0].read_text(encoding="utf-8").splitlines()
    for line in lines:
        record = json.loads(line)
        assert "discovery_slug_hits" not in record
        assert "discovery_listing_hits" not in record
        assert "rtds_dropped_not_json" not in record
        assert "rtds_dropped_unknown_symbol" not in record
        assert "rtds_dropped_unknown_shape" not in record
