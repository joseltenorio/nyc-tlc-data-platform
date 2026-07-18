from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from tlc_data_platform.core.settings import AuditConfig


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class UnifiedAuditRepository:
    """Write-once operational facts shared by Bronze, Silver, Gold, ML and Streamlit.

    The existing layer-specific collections remain intact. These collections add a
    stable cross-layer contract for dashboards: runs, datasets, data-quality,
    coverage and every HTTP download attempt.
    """

    def __init__(self, database: Any, config: AuditConfig) -> None:
        self._db = database
        self._config = config
        self._names = config.collections
        self.ensure_indexes()

    def ensure_indexes(self) -> None:
        self._db[self._names.pipeline_runs].create_index(
            [("execution_id", 1)], unique=True, name="uq_audit_pipeline_execution"
        )
        self._db[self._names.pipeline_runs].create_index(
            [("layer", 1), ("started_at", -1)], name="ix_audit_runs_layer_started"
        )
        self._db[self._names.dataset_events].create_index(
            [("event_id", 1)], unique=True, name="uq_audit_dataset_event"
        )
        self._db[self._names.dataset_events].create_index(
            [("execution_id", 1), ("layer", 1), ("dataset_name", 1)],
            name="ix_audit_dataset_execution",
        )
        self._db[self._names.quality_events].create_index(
            [("quality_id", 1)], unique=True, name="uq_audit_quality_event"
        )
        self._db[self._names.quality_events].create_index(
            [("execution_id", 1), ("layer", 1), ("status", 1)],
            name="ix_audit_quality_execution",
        )
        self._db[self._names.coverage_snapshots].create_index(
            [("execution_id", 1), ("layer", 1)],
            unique=True,
            name="uq_audit_coverage_execution_layer",
        )
        self._db[self._names.download_attempts].create_index(
            [
                ("execution_id", 1),
                ("service", 1),
                ("year", 1),
                ("month", 1),
                ("attempt_number", 1),
            ],
            unique=True,
            name="uq_audit_download_attempt",
        )
        self._db[self._names.download_attempts].create_index(
            [("attempted_at", -1), ("outcome", 1)], name="ix_audit_download_attempt_time"
        )

    def start_run(
        self,
        execution_id: str,
        *,
        layer: str,
        execution_type: str,
        selection: dict[str, Any] | None = None,
        parent_execution_id: str | None = None,
        started_at: datetime | None = None,
    ) -> None:
        now = started_at or utc_now()
        self._db[self._names.pipeline_runs].update_one(
            {"execution_id": execution_id},
            {
                "$set": {
                    "execution_id": execution_id,
                    "parent_execution_id": parent_execution_id,
                    "layer": layer,
                    "execution_type": execution_type,
                    "status": "RUNNING",
                    "started_at": now,
                    "selection": selection or {},
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )

    def link_parent(self, execution_id: str, parent_execution_id: str) -> None:
        """Links an already-created layer run to its platform/orchestrator run."""
        self._db[self._names.pipeline_runs].update_one(
            {"execution_id": execution_id},
            {
                "$set": {
                    "parent_execution_id": parent_execution_id,
                    "updated_at": utc_now(),
                }
            },
            upsert=False,
        )

    def finish_run(
        self,
        execution_id: str,
        *,
        status: str,
        metrics: dict[str, Any] | None = None,
        warnings: list[str] | None = None,
        finished_at: datetime | None = None,
    ) -> None:
        now = finished_at or utc_now()
        document = self._db[self._names.pipeline_runs].find_one(
            {"execution_id": execution_id}, {"started_at": 1}
        ) or {}
        started = document.get("started_at")
        duration = (now - started).total_seconds() if isinstance(started, datetime) else None
        self._db[self._names.pipeline_runs].update_one(
            {"execution_id": execution_id},
            {
                "$set": {
                    "status": status,
                    "finished_at": now,
                    "duration_seconds": duration,
                    "metrics": metrics or {},
                    "warnings": warnings or [],
                    "updated_at": now,
                }
            },
            upsert=True,
        )

    def fail_run(self, execution_id: str, error: Exception, *, layer: str | None = None) -> None:
        now = utc_now()
        existing = self._db[self._names.pipeline_runs].find_one(
            {"execution_id": execution_id}, {"started_at": 1}
        ) or {}
        started_at = existing.get("started_at")
        duration_seconds = (
            (now - started_at).total_seconds()
            if isinstance(started_at, datetime)
            else None
        )
        self._db[self._names.pipeline_runs].update_one(
            {"execution_id": execution_id},
            {
                "$set": {
                    "layer": layer,
                    "status": "FAILED",
                    "finished_at": now,
                    "duration_seconds": duration_seconds,
                    "error_type": type(error).__name__,
                    "error_message": str(error)[:4000],
                    "updated_at": now,
                }
            },
            upsert=True,
        )

    def record_dataset(
        self,
        execution_id: str,
        *,
        layer: str,
        dataset_name: str,
        dataset_type: str,
        operation: str,
        status: str,
        path: str | None = None,
        parquet_files: int | None = None,
        rows: int | None = None,
        bytes_on_disk: int | None = None,
        service: str | None = None,
        year: int | None = None,
        month: int | None = None,
        source_dataset: str | None = None,
        error: Exception | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        event_id = str(uuid4())
        payload: dict[str, Any] = {
            "event_id": event_id,
            "execution_id": execution_id,
            "layer": layer,
            "dataset_name": dataset_name,
            "dataset_type": dataset_type,
            "operation": operation,
            "status": status,
            "path": path,
            "parquet_files": parquet_files,
            "rows": rows,
            "bytes_on_disk": bytes_on_disk,
            "service": service,
            "year": year,
            "month": month,
            "source_dataset": source_dataset,
            "metadata": metadata or {},
            "recorded_at": utc_now(),
        }
        if error is not None:
            payload["error_type"] = type(error).__name__
            payload["error_message"] = str(error)[:4000]
        self._db[self._names.dataset_events].insert_one(payload)
        return event_id

    def record_quality(
        self,
        execution_id: str,
        *,
        layer: str,
        dataset_name: str,
        rule_code: str,
        dimension: str,
        severity: str,
        status: str,
        expected: Any = None,
        actual: Any = None,
        failed_rows: int | None = None,
        message: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> str:
        quality_id = str(uuid4())
        self._db[self._names.quality_events].insert_one(
            {
                "quality_id": quality_id,
                "execution_id": execution_id,
                "layer": layer,
                "dataset_name": dataset_name,
                "rule_code": rule_code,
                "dimension": dimension,
                "severity": severity,
                "status": status,
                "expected": expected,
                "actual": actual,
                "failed_rows": failed_rows,
                "message": message,
                "context": context or {},
                "checked_at": utc_now(),
            }
        )
        return quality_id

    def record_coverage(
        self,
        execution_id: str,
        *,
        layer: str,
        expected_count: int,
        available_count: int,
        ready_count: int,
        missing: list[str],
        not_applicable_count: int = 0,
        not_published_count: int = 0,
        deferred_count: int = 0,
        details: list[dict[str, Any]] | None = None,
    ) -> None:
        excluded_not_published = (
            not_published_count
            if not self._config.treat_not_published_as_missing
            else 0
        )
        denominator = max(
            0, expected_count - not_applicable_count - excluded_not_published
        )
        rate = ready_count / denominator if denominator else 1.0
        status = "COMPLETE" if not missing and ready_count >= denominator else "PARTIAL"
        self._db[self._names.coverage_snapshots].update_one(
            {"execution_id": execution_id, "layer": layer},
            {
                "$set": {
                    "execution_id": execution_id,
                    "layer": layer,
                    "status": status,
                    "expected_count": expected_count,
                    "available_count": available_count,
                    "ready_count": ready_count,
                    "missing_count": len(missing),
                    "not_applicable_count": not_applicable_count,
                    "not_published_count": not_published_count,
                    "deferred_count": deferred_count,
                    "coverage_rate": rate,
                    "missing": missing,
                    "details": details or [],
                    "checked_at": utc_now(),
                }
            },
            upsert=True,
        )

    def record_download_attempt(
        self,
        execution_id: str,
        *,
        service: str,
        year: int,
        month: int,
        url: str,
        attempt_number: int,
        max_attempts: int,
        outcome: str,
        status_code: int | None = None,
        retry_delay_seconds: float | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> None:
        payload = {
            "execution_id": execution_id,
            "layer": "bronze",
            "service": service,
            "year": year,
            "month": month,
            "url": url,
            "attempt_number": attempt_number,
            "retry_number": max(0, attempt_number - 1),
            "max_attempts": max_attempts,
            "outcome": outcome,
            "status_code": status_code,
            "retry_delay_seconds": retry_delay_seconds,
            "error_type": error_type,
            "error_message": error_message,
            "attempted_at": utc_now(),
        }
        self._db[self._names.download_attempts].update_one(
            {
                "execution_id": execution_id,
                "service": service,
                "year": year,
                "month": month,
                "attempt_number": attempt_number,
            },
            {"$set": payload},
            upsert=True,
        )
