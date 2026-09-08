import json

from collector.state import LongjobState, load_state, save_state


def test_load_state_missing_file_returns_defaults(tmp_path):
    state = load_state(tmp_path / "nope.json")
    assert state == LongjobState()


def test_save_then_load_roundtrip(tmp_path):
    path = tmp_path / "state" / "longjob.json"
    save_state(LongjobState(last_processed_round_epoch_s=1717000200, updated_at_ms=1717000205000), path)

    loaded = load_state(path)
    assert loaded.last_processed_round_epoch_s == 1717000200
    assert loaded.updated_at_ms == 1717000205000


def test_saved_file_notes_it_is_derived_and_out_of_schema_scope(tmp_path):
    path = tmp_path / "longjob.json"
    save_state(LongjobState(), path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert "SCHEMA.md" in payload["_note"]


def test_load_state_corrupt_json_falls_back_to_defaults(tmp_path):
    path = tmp_path / "longjob.json"
    path.write_text("{not valid json", encoding="utf-8")
    assert load_state(path) == LongjobState()
