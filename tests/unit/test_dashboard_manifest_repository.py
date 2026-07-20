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


def test_manifest_prefers_recorded_error_rate_over_inference(tmp_path: Path):
    root = tmp_path / "manifests"
    bronze = root / "bronze"
    bronze.mkdir(parents=True)
    path = bronze / "run-rate.json"
    path.write_text(
        json.dumps(
            {
                "layer": "bronze",
                "summary": {
                    "execution_id": "run-rate",
                    "applicable_periods": 10,
                    "failed_files": 1,
                    "error_rate": 0.25,
                    "total_download_seconds": 12.5,
                    "average_download_mbps": 48.2,
                },
            }
        ),
        encoding="utf-8",
    )

    row, _ = _normalize_manifest(path, root)

    assert row["error_rate"] == 0.25
    assert row["total_download_seconds"] == 12.5
    assert row["average_download_mbps"] == 48.2


def test_manifest_does_not_infer_zero_error_rate_without_failed_count(tmp_path: Path):
    root = tmp_path / "manifests"
    root.mkdir()
    path = root / "run-missing-failures.json"
    path.write_text(
        json.dumps(
            {
                "summary": {
                    "execution_id": "run-missing-failures",
                    "applicable_periods": 10,
                    "downloaded_files": 10,
                }
            }
        ),
        encoding="utf-8",
    )

    row, _ = _normalize_manifest(path, root)

    assert row["error_rate"] is None


def test_manifest_exposes_real_file_level_failures(tmp_path: Path):
    root = tmp_path / "manifests"
    bronze = root / "bronze"
    bronze.mkdir(parents=True)
    path = bronze / "run-file-error.json"
    path.write_text(
        json.dumps(
            {
                "layer": "bronze",
                "summary": {
                    "execution_id": "run-file-error",
                    "status": "PARTIAL_SUCCESS",
                    "finished_at": "2026-07-18T10:01:00+00:00",
                    "failed_files": 1,
                },
                "files": [
                    {
                        "file_name": "yellow_tripdata_2025-01.parquet",
                        "service": "yellow",
                        "year": 2025,
                        "month": 1,
                        "status": "FAILED",
                        "error_type": "TimeoutError",
                        "error_message": "download timed out",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    _, errors = _normalize_manifest(path, root)

    assert len(errors) == 1
    assert errors[0]["subject"] == "yellow_tripdata_2025-01.parquet"
    assert errors[0]["service"] == "yellow"
    assert errors[0]["error_type"] == "TimeoutError"


def test_manifest_reads_nested_gold_result_failures(tmp_path: Path):
    root = tmp_path / "manifests"
    gold = root / "gold"
    gold.mkdir(parents=True)
    path = gold / "gold-run-error.json"
    path.write_text(
        json.dumps(
            {
                "layer": "gold",
                "summary": {
                    "execution_id": "gold-run-error",
                    "status": "FAILED",
                    "results": [
                        {
                            "dataset_name": "fact_trip_activity",
                            "status": "FAILED",
                            "error_type": "ValueError",
                            "error_message": "row reconciliation failed",
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    _, errors = _normalize_manifest(path, root)

    assert len(errors) == 1
    assert errors[0]["subject"] == "fact_trip_activity"
    assert errors[0]["error_message"] == "row reconciliation failed"
