from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class SilverSourceFile:
    service: str
    year: int
    month: int
    path: Path
    source_sha256: str | None
    bronze_execution_id: str | None
    bronze_num_rows: int | None
    bronze_registry_status: str

    @property
    def period_id(self) -> str:
        return f"{self.service}:{self.year}-{self.month:02d}"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["path"] = str(self.path)
        return payload


@dataclass(frozen=True)
class SilverPeriodState:
    service: str
    year: int
    month: int
    status: str
    source_path: str | None = None
    source_sha256: str | None = None
    detail: str | None = None

    @property
    def period_id(self) -> str:
        return f"{self.service}:{self.year}-{self.month:02d}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SilverFileOutcome:
    source: SilverSourceFile
    status: str
    started_at: datetime = field(default_factory=utc_now)
    finished_at: datetime | None = None
    curated_path: str | None = None
    rejected_path: str | None = None
    master_path: str | None = None
    rows_read: int = 0
    rows_valid: int = 0
    rows_rejected: int = 0
    warning_rows: int = 0
    rule_counts: dict[str, int] = field(default_factory=dict)
    rule_severities: dict[str, str] = field(default_factory=dict)
    reconciliation_status: str | None = None
    error_type: str | None = None
    error_message: str | None = None

    def finish(self) -> None:
        self.finished_at = utc_now()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["source"] = self.source.to_dict()
        return payload


@dataclass(frozen=True)
class SilverPlanSummary:
    services: list[str]
    start_year: int
    end_year: int
    months: list[int]
    expected_periods: int
    bronze_ready_periods: int
    bronze_missing_periods: int
    already_processed_periods: int
    pending_periods: int
    states: list[dict[str, Any]]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SilverExecutionSummary:
    execution_id: str
    execution_type: str
    status: str
    started_at: datetime
    finished_at: datetime
    requested_services: list[str]
    requested_start_year: int
    requested_end_year: int
    requested_months: list[int]
    source_files: int
    processed_files: int
    skipped_files: int
    failed_files: int
    rows_read: int
    rows_valid: int
    rows_rejected: int
    warning_rows: int
    manifest_path: str
    reference_refresh_status: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SilverTransformContext:
    service: str
    year: int
    month: int
    source_file: str
    source_sha256: str | None
    bronze_execution_id: str | None
    silver_execution_id: str


@dataclass(frozen=True)
class ReferenceRefreshSummary:
    status: str
    taxi_zones_rows: int
    base_lookup_rows: int
    taxi_zones_path: str
    base_lookup_path: str
    taxi_zones_bronze_path: str
    base_lookup_bronze_path: str
    taxi_zones_sha256: str
    base_lookup_sha256: str
    manifest_path: str
    refreshed_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
