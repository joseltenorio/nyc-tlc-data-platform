from __future__ import annotations

from datetime import datetime, timezone

import tlc_data_platform.orchestration.medallion_pipeline as module
from tlc_data_platform.bronze.models import ExecutionSummary


def bronze_summary(status: str) -> ExecutionSummary:
    now = datetime.now(timezone.utc)
    return ExecutionSummary(
        execution_id="bronze-run",
        execution_type="HISTORICAL",
        status=status,
        started_at=now,
        finished_at=now,
        requested_services=["yellow"],
        requested_start_year=2023,
        requested_end_year=2025,
        requested_months=list(range(1, 13)),
        expected_periods=36,
        applicable_periods=36,
        available_files=36,
        downloaded_files=1,
        ready_files=1,
        skipped_files=35,
        failed_files=0,
        failed_probe_periods=0,
        not_published_files=0,
        not_applicable_periods=0,
        total_bytes_downloaded=1,
        manifest_path="data/manifests/bronze/run.json",
    )


def test_partial_bronze_does_not_start_silver(monkeypatch):
    calls = {"silver": 0}
    monkeypatch.setattr(
        module,
        "run_bronze_pipeline",
        lambda *args, **kwargs: bronze_summary("PARTIAL_SUCCESS"),
    )

    def fake_silver(*args, **kwargs):
        calls["silver"] += 1
        raise AssertionError("Silver must not start with incomplete Bronze")

    monkeypatch.setattr(module, "run_silver_pipeline", fake_silver)

    result = module.run_medallion_to_silver(
        object(),
        object(),
        execution_type="historical",
    )

    assert result.status == "PARTIAL_SUCCESS"
    assert result.silver is None
    assert calls["silver"] == 0


def test_failed_bronze_does_not_start_silver(monkeypatch):
    monkeypatch.setattr(
        module,
        "run_bronze_pipeline",
        lambda *args, **kwargs: bronze_summary("FAILED"),
    )
    monkeypatch.setattr(
        module,
        "run_silver_pipeline",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("Silver must not start after failed Bronze")
        ),
    )

    result = module.run_medallion_to_silver(
        object(),
        object(),
        execution_type="historical",
    )

    assert result.status == "FAILED"
    assert result.silver is None
