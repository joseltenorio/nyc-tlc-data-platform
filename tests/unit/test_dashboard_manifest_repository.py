from __future__ import annotations

import json
from pathlib import Path

from dashboard.data_access.manifest_repository import _normalize_manifest


def test_bronze_manifest_is_normalized_without_inventing_rows(tmp_path: Path):
    root = tmp_path / "manifests"
    root.mkdir()
    path = root / "run-test.json"
    path.write_text(
        json.dumps(
            {
                "summary": {
                    "execution_id": "run-test",
                    "execution_type": "RUN",
                    "status": "SUCCESS",
                    "started_at": "2026-07-17T10:00:00+00:00",
                    "finished_at": "2026-07-17T10:01:00+00:00",
                    "applicable_periods": 2,
                    "downloaded_files": 2,
                    "failed_files": 0,
                }
            }
        ),
        encoding="utf-8",
    )

    row, errors = _normalize_manifest(path, root)

    assert row["layer"] == "bronze"
    assert row["status"] == "SUCCESS"
    assert row["processed_files"] == 2
    assert row["rows_input"] is None
    assert row["duration_seconds"] == 60
    assert errors == []


def test_silver_manifest_uses_real_quality_counts(tmp_path: Path):
    root = tmp_path / "manifests"
    silver = root / "silver"
    silver.mkdir(parents=True)
    path = silver / "silver-test.json"
    path.write_text(
        json.dumps(
            {
                "summary": {
                    "execution_id": "silver-test",
                    "status": "PARTIAL_SUCCESS",
                    "source_files": 10,
                    "processed_files": 9,
                    "failed_files": 1,
                    "rows_read": 1000,
                    "rows_valid": 970,
                    "rows_rejected": 30,
                    "warning_rows": 50,
                }
            }
        ),
        encoding="utf-8",
    )

    row, _ = _normalize_manifest(path, root)

    assert row["layer"] == "silver"
    assert row["rows_input"] == 1000
    assert row["rows_valid"] == 970
    assert row["rows_rejected"] == 30
    assert row["error_rate"] == 0.1
