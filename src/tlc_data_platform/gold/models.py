"""Define los contratos de planificación, resultados y resumen de la capa Gold."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class GoldPlanSummary:
    status: str
    selected_partitions: int
    available_partitions: int
    missing_partitions: list[str]
    source_paths: list[str]
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GoldDatasetResult:
    dataset_name: str
    dataset_type: str
    path: str
    status: str
    rows_written: int | None
    partition_columns: tuple[str, ...] = ()
    built_at: datetime = field(default_factory=utc_now)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["partition_columns"] = list(self.partition_columns)
        return payload


@dataclass(frozen=True)
class GoldExecutionSummary:
    execution_id: str
    execution_type: str
    status: str
    started_at: datetime
    finished_at: datetime
    selected_start_year: int
    selected_end_year: int
    selected_months: list[int]
    selected_services: list[str]
    source_partitions: int
    source_rows: int
    datasets_built: int
    failed_datasets: int
    results: list[GoldDatasetResult]
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["results"] = [result.to_dict() for result in self.results]
        return payload
