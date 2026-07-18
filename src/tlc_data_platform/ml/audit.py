"""Agrupa los repositorios de auditoría utilizados durante entrenamiento y scoring ML."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from tlc_data_platform.audit.ml_model_repository import MLModelRepository
from tlc_data_platform.audit.ml_run_repository import MLRunRepository
from tlc_data_platform.audit.processing_attempt_repository import ProcessingAttemptRepository


@dataclass(frozen=True)
class MLAuditRepositories:
    runs: MLRunRepository
    models: MLModelRepository
    attempts: ProcessingAttemptRepository
    prediction_runs: Any
    metrics: Any
    unified: Any | None = None
