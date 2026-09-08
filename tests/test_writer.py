import json

from collector import writer
from tests.fixtures import VALID_HEARTBEAT, VALID_ROUND


def test_write_round_valid_appends_to_expected_path(tmp_path):
    record = dict(VALID_ROUND)
    path = writer.write_round(record, base_dir=tmp_path / "raw")

    assert path == tmp_path / "raw" / "runner=longjob" / "date=2024-05-29" / "rounds.jsonl"
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["round_id"] == record["round_id"]


def test_write_round_appends_multiple_lines(tmp_path):
    base_dir = tmp_path / "raw"
    writer.write_round(dict(VALID_ROUND), base_dir=base_dir)
    writer.write_round(dict(VALID_ROUND), base_dir=base_dir)
    path = base_dir / "runner=longjob" / "date=2024-05-29" / "rounds.jsonl"
    assert len(path.read_text(encoding="utf-8").splitlines()) == 2


def test_write_round_invalid_goes_to_rejected_not_raw(tmp_path):
    broken = dict(VALID_ROUND)
    del broken["condition_id"]  # zorunlu alan eksik

    raw_dir = tmp_path / "raw"
    rejected_dir = tmp_path / "rejected"
    path = writer.write_round(broken, base_dir=raw_dir, rejected_base_dir=rejected_dir)

    assert not raw_dir.exists()
    assert path.parent.parent.parent == rejected_dir
    entry = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    assert entry["record_type"] == "round"
    assert any("condition_id" in e for e in entry["errors"])


def test_write_heartbeat_valid(tmp_path):
    record = dict(VALID_HEARTBEAT)
    path = writer.write_heartbeat(record, base_dir=tmp_path / "coverage")
    assert path.exists()
    assert json.loads(path.read_text(encoding="utf-8").splitlines()[0])["event"] == "tick"


def test_write_heartbeat_invalid_goes_to_rejected(tmp_path):
    broken = dict(VALID_HEARTBEAT)
    broken["event"] = "not_a_real_event"

    rejected_dir = tmp_path / "rejected"
    path = writer.write_heartbeat(broken, base_dir=tmp_path / "coverage", rejected_base_dir=rejected_dir)
    assert path.parent.parent.parent == rejected_dir
