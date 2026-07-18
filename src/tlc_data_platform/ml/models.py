"""Define los contratos de resultados, métricas y resúmenes de Machine Learning."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class MLPlanSummary:
    status: str
    requested_models: list[str]
    available_features: dict[str, bool]
    missing_paths: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MLModelResult:
    model_id: str
    model_name: str
    algorithm: str
    status: str
    trained_at: datetime
    training_rows: int
    validation_rows: int
    test_rows: int
    metrics: dict[str, float]
    model_path: str
    output_paths: dict[str, str]
    feature_columns: list[str]
    target_column: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MLExecutionSummary:
    run_id: str
    status: str
    started_at: datetime
    finished_at: datetime
    requested_models: list[str]
    trained_models: int
    failed_models: int
    results: list[MLModelResult]
    errors: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["results"] = [result.to_dict() for result in self.results]
        return payload
