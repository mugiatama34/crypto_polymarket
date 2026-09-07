import json

from validator.reject import write_rejected


def test_writes_to_runner_date_path(tmp_path):
    raw_line = '{"schema_version": 1, "broken": true'  # kasitli bozuk JSON, string olarak saklanir
    path = write_rejected(
        raw_line,
        ["'round_id' is a required property"],
        runner_id="longjob",
        record_type="round",
        job_id="job-abc123",
        ts=1717000000000,
        base_dir=tmp_path,
    )

    assert path == tmp_path / "runner=longjob" / "date=2024-05-29" / "rejected.jsonl"
    assert path.exists()


def test_raw_line_stored_unmodified(tmp_path):
    raw_line = '{"weird":   "spacing",\n"trailing": "comma",}'
    path = write_rejected(
        raw_line,
        ["invalid json"],
        runner_id="cron",
        base_dir=tmp_path,
    )

    written = json.loads(path.read_text(encoding="utf-8").strip())
    assert written["raw_line"] == raw_line


def test_entry_carries_required_fields(tmp_path):
    path = write_rejected(
        '{"schema_version": 1}',
        ["missing fields"],
        runner_id="reconciler",
        record_type="outcome",
        job_id=None,
        ts=1717000000000,
        base_dir=tmp_path,
    )

    entry = json.loads(path.read_text(encoding="utf-8").strip())
    assert entry["ts"] == 1717000000000
    assert entry["runner_id"] == "reconciler"
    assert entry["job_id"] is None
    assert entry["record_type"] == "outcome"
    assert entry["errors"] == ["missing fields"]


def test_appends_rather_than_overwrites(tmp_path):
    path1 = write_rejected(
        '{"a": 1}', ["err1"], runner_id="longjob", ts=1717000000000, base_dir=tmp_path
    )
    path2 = write_rejected(
        '{"a": 2}', ["err2"], runner_id="longjob", ts=1717000000000, base_dir=tmp_path
    )

    assert path1 == path2
    lines = path1.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["raw_line"] == '{"a": 1}'
    assert json.loads(lines[1])["raw_line"] == '{"a": 2}'


def test_different_runners_write_different_files(tmp_path):
    path_longjob = write_rejected(
        '{"a": 1}', ["err"], runner_id="longjob", ts=1717000000000, base_dir=tmp_path
    )
    path_cron = write_rejected(
        '{"a": 1}', ["err"], runner_id="cron", ts=1717000000000, base_dir=tmp_path
    )
    path_reconciler = write_rejected(
        '{"a": 1}', ["err"], runner_id="reconciler", ts=1717000000000, base_dir=tmp_path
    )

    assert len({path_longjob, path_cron, path_reconciler}) == 3
