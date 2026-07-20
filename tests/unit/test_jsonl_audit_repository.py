from __future__ import annotations

import json
from pathlib import Path

from dashboard.data_access.jsonl_audit_repository import (
    _coalesce_runs,
    _load_inventory,
    _records_for,
)


def write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )


def test_run_events_are_merged_chronologically_without_losing_known_layer(tmp_path: Path):
    root = tmp_path / "audit"
    write_jsonl(
        root / "unknown" / "pipeline_runs.jsonl",
        [
            {
                "execution_id": "run-1",
                "layer": "unknown",
                "parent_execution_id": "platform-1",
                "written_at": "2026-07-18T10:00:02+00:00",
            }
        ],
    )
    write_jsonl(
        root / "bronze" / "pipeline_runs.jsonl",
        [
            {
                "execution_id": "run-1",
                "layer": "bronze",
                "status": "RUNNING",
                "started_at": "2026-07-18T10:00:00+00:00",
                "written_at": "2026-07-18T10:00:00+00:00",
            },
            {
                "execution_id": "run-1",
                "layer": "bronze",
                "status": "SUCCESS",
                "finished_at": "2026-07-18T10:00:05+00:00",
                "metrics": {"error_rate": 0.0},
                "written_at": "2026-07-18T10:00:05+00:00",
            },
        ],
    )

    records = _records_for(root, "pipeline_runs.jsonl", 100)
    frame = _coalesce_runs(records)

    assert len(frame) == 1
    assert frame.loc[0, "layer"] == "bronze"
    assert frame.loc[0, "status"] == "SUCCESS"
    assert frame.loc[0, "parent_execution_id"] == "platform-1"
    assert frame.loc[0, "duration_seconds"] == 5.0
    assert frame.loc[0, "metrics.error_rate"] == 0.0


def test_current_inventory_is_loaded_without_estimating_missing_layers(tmp_path: Path):
    root = tmp_path / "audit"
    path = root / "inventory" / "medallion_inventory.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "snapshot_id": "snapshot-1",
                "execution_id": "run-1",
                "captured_at": "2026-07-18T10:00:00+00:00",
                "layers": [
                    {
                        "layer": "bronze",
                        "parquet_files": 3,
                        "bytes_on_disk": 100,
                        "dataset_count": 2,
                        "datasets": [
                            {
                                "dataset_name": "yellow",
                                "parquet_files": 2,
                                "bytes_on_disk": 80,
                            }
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    frame = _load_inventory(root, "medallion_inventory.json")

    assert frame["layer"].tolist() == ["bronze"]
    assert frame.loc[0, "parquet_files"] == 3
    assert "silver" not in frame["layer"].tolist()
