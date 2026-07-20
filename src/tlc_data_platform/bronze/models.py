from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFERRED_REMOTE_ACCESS = "DEFERRED_REMOTE_ACCESS"
NOT_PUBLISHED_STATUS_CODES = frozenset({404, 410})


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, order=True)
class ExpectedPeriod:
    service: str
    year: int
    month: int
    applicable: bool
    expected: bool = True

    @property
    def period_id(self) -> str:
        return f"{self.service}:{self.year}-{self.month:02d}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, order=True)
class FileCandidate:
    service: str
    year: int
    month: int
    url: str
    file_name: str
    discovery_method: str

    @property
    def period_id(self) -> str:
        return f"{self.service}:{self.year}-{self.month:02d}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RemoteMetadata:
    available: bool
    status_code: int | None = None
    content_length: int | None = None
    etag: str | None = None
    last_modified: str | None = None
    content_type: str | None = None
    probe_failed: bool = False
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def classify_remote_availability(remote: RemoteMetadata) -> str:
    if remote.available:
        return "AVAILABLE"
    if remote.probe_failed:
        return "FAILED_TO_PROBE"
    if remote.status_code in NOT_PUBLISHED_STATUS_CODES:
        return "NOT_PUBLISHED_YET"
    return DEFERRED_REMOTE_ACCESS


@dataclass
class AvailabilityRecord:
    execution_id: str
    service: str
    year: int
    month: int
    status: str
    applicable: bool
    expected: bool
    candidate_url: str | None = None
    discovery_method: str | None = None
    remote_metadata: RemoteMetadata | None = None
    checked_at: datetime = field(default_factory=utc_now)

    @property
    def period_id(self) -> str:
        return f"{self.service}:{self.year}-{self.month:02d}"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if self.remote_metadata is not None:
            payload["remote_metadata"] = self.remote_metadata.to_dict()
        return payload


@dataclass(frozen=True)
class DiscoveryResult:
    expected_periods: list[ExpectedPeriod]
    candidates: list[FileCandidate]
    availability: list[AvailabilityRecord]
    html_error: str | None = None


@dataclass(frozen=True)
class DownloadResult:
    candidate: FileCandidate
    path: Path
    bytes_downloaded: int
    sha256: str
    remote_metadata: RemoteMetadata
    attempt_count: int = 1
    retry_count: int = 0
    download_started_at: datetime | None = None
    download_finished_at: datetime | None = None
    download_duration_seconds: float | None = None
    throughput_bytes_per_second: float | None = None


@dataclass(frozen=True)
class ValidationResult:
    expected_required_columns: list[str]
    expected_optional_columns: list[str]
    observed_columns: list[str]
    missing_required_columns: list[str]
    missing_optional_columns: list[str]
    new_columns: list[str]
    observed_types: dict[str, str]
    required_field_matches: dict[str, str]
    optional_field_matches: dict[str, str]
    type_mismatches: dict[str, dict[str, Any]]
    schema_json: str
    schema_hash: str
    schema_evolution_detected: bool
    schema_events: list[str]
    parquet_num_rows: int
    parquet_num_row_groups: int
    parquet_num_columns: int
    parquet_created_by: str | None
    parquet_compression_codecs: list[str]
    sample_rows_read: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class FileOutcome:
    candidate: FileCandidate
    status: str
    started_at: datetime = field(default_factory=utc_now)
    finished_at: datetime | None = None
    remote_metadata: RemoteMetadata | None = None
    local_path: str | None = None
    archived_previous_path: str | None = None
    bytes_downloaded: int | None = None
    sha256: str | None = None
    validation: ValidationResult | None = None
    error_type: str | None = None
    error_message: str | None = None
    attempt_count: int = 0
    retry_count: int = 0
    download_started_at: datetime | None = None
    download_finished_at: datetime | None = None
    download_duration_seconds: float | None = None
    throughput_bytes_per_second: float | None = None

    def finish(self) -> None:
        self.finished_at = utc_now()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["candidate"] = self.candidate.to_dict()
        if self.remote_metadata is not None:
            payload["remote_metadata"] = self.remote_metadata.to_dict()
        if self.validation is not None:
            payload["validation"] = self.validation.to_dict()
        return payload


@dataclass(frozen=True)
class PlanSummary:
    plan_type: str
    services: list[str]
    start_year: int
    end_year: int
    months: list[int]
    expected_periods: int
    applicable_periods: int
    not_applicable_periods: int
    available_files: int
    not_published_files: int
    failed_probes: int
    already_processed_files: int
    pending_files: int
    estimated_remote_bytes: int
    unknown_size_files: int
    free_space_bytes: int
    minimum_free_space_bytes: int
    workers: int
    max_hvfhv_workers: int
    warnings: list[str]
    candidates: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionSummary:
    execution_id: str
    execution_type: str
    status: str
    started_at: datetime
    finished_at: datetime
    requested_services: list[str]
    requested_start_year: int
    requested_end_year: int
    requested_months: list[int]
    expected_periods: int
    applicable_periods: int
    available_files: int
    downloaded_files: int
    ready_files: int
    skipped_files: int
    failed_files: int
    failed_probe_periods: int
    not_published_files: int
    not_applicable_periods: int
    total_bytes_downloaded: int
    manifest_path: str
    total_download_attempts: int = 0
    total_retries: int = 0
    total_download_seconds: float | None = None
    average_download_mbps: float | None = None
    error_rate: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
