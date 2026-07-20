from __future__ import annotations

import pandas as pd

from dashboard.data_access.audit_repository import _merge_quality


def test_legacy_silver_quality_is_preserved_without_duplicating_unified_event():
    unified = pd.DataFrame(
        [
            {
                "quality_id": "quality-1",
                "execution_id": "silver-1",
                "layer": "silver",
                "dataset_name": "yellow_trips",
                "rule_code": "NEGATIVE_AMOUNT",
                "severity": "ERROR",
                "status": "FAILED",
                "failed_rows": 3,
                "context.service": "yellow",
                "context.year": 2025,
                "context.month": 1,
                "checked_at": "2026-07-18T10:00:01+00:00",
            }
        ]
    )
    legacy = pd.DataFrame(
        [
            {
                "execution_id": "silver-1",
                "service": "yellow",
                "year": 2025,
                "month": 1,
                "rule_code": "NEGATIVE_AMOUNT",
                "severity": "ERROR",
                "affected_rows": 3,
                "recorded_at": "2026-07-18T10:00:00+00:00",
            },
            {
                "execution_id": "silver-old",
                "service": "green",
                "year": 2024,
                "month": 2,
                "rule_code": "NULL_LOCATION",
                "severity": "WARNING",
                "affected_rows": 5,
                "recorded_at": "2026-07-17T10:00:00+00:00",
            },
        ]
    )

    result = _merge_quality(unified, pd.DataFrame(), legacy, pd.DataFrame())

    assert len(result) == 2
    current = result[result["execution_id"] == "silver-1"].iloc[0]
    historical = result[result["execution_id"] == "silver-old"].iloc[0]
    assert current["quality_id"] == "quality-1"
    assert current["status"] == "FAILED"
    assert historical["status"] == "WARNING"
    assert historical["failed_rows"] == 5
    assert historical["layer"] == "silver"


def test_legacy_quality_without_status_or_affected_rows_stays_unknown():
    legacy = pd.DataFrame(
        [
            {
                "execution_id": "silver-unknown",
                "service": "yellow",
                "year": 2025,
                "month": 1,
                "rule_code": "UNRECORDED_RESULT",
                "severity": "ERROR",
                "recorded_at": "2026-07-18T10:00:00+00:00",
            }
        ]
    )

    result = _merge_quality(pd.DataFrame(), pd.DataFrame(), legacy, pd.DataFrame())

    assert len(result) == 1
    assert result.iloc[0]["status"] == "UNKNOWN"
    assert pd.isna(result.iloc[0]["failed_rows"])


def test_newer_jsonl_run_state_overrides_stale_mongo_state():
    from dashboard.data_access.audit_repository import _coalesce_frames

    mongo = pd.DataFrame(
        [
            {
                "execution_id": "run-stale",
                "layer": "bronze",
                "status": "RUNNING",
                "started_at": "2026-07-18T10:00:00+00:00",
                "updated_at": "2026-07-18T10:00:01+00:00",
                "selection.services": ["yellow"],
            }
        ]
    )
    jsonl = pd.DataFrame(
        [
            {
                "execution_id": "run-stale",
                "layer": "bronze",
                "status": "SUCCESS",
                "started_at": "2026-07-18T10:00:00+00:00",
                "finished_at": "2026-07-18T10:00:10+00:00",
                "updated_at": "2026-07-18T10:00:10+00:00",
                "duration_seconds": 10.0,
            }
        ]
    )

    result = _coalesce_frames(
        [mongo, jsonl], keys=["execution_id"], sort_column="started_at"
    )

    assert result.loc[0, "status"] == "SUCCESS"
    assert result.loc[0, "duration_seconds"] == 10.0
    assert result.loc[0, "selection.services"] == ["yellow"]


def test_consolidated_errors_are_not_duplicated_across_sources():
    from dashboard.data_access.audit_repository import _dedupe_errors

    frame = pd.DataFrame(
        [
            {
                "execution_id": "run-1",
                "layer": "bronze",
                "error_type": "TimeoutError",
                "error_message": "download timed out",
                "source": "manifest",
            },
            {
                "execution_id": "run-1",
                "layer": "bronze",
                "error_type": "TimeoutError",
                "error_message": "download timed out",
                "source": "pipeline_run",
            },
        ]
    )

    result = _dedupe_errors(frame)

    assert len(result) == 1
