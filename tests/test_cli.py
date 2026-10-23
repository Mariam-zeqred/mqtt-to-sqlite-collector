import json

from collector.__main__ import main

SAMPLE = "data/sample_incubator.jsonl"


def test_replay_sample_then_compact_and_plan(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("RAW_DAYS", "7")  # the sample is from 2026-10-03, so it is older than a week
    db = tmp_path / "cli.db"
    assert main(["--db", str(db), "replay", SAMPLE]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["outcomes"] == {"stored": 1368}  # 1 367 readings + 1 status message, nothing rejected
    assert out["rows"]["readings"] == 1367 and out["rows"]["status"] == 1

    assert main(["--db", str(db), "compact"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["readings_rolled_up"] == 1367

    assert main(["--db", str(db), "plan"]) == 0
    assert "| 1000 |" in capsys.readouterr().out
