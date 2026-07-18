"""Agrupa los repositorios de auditoría utilizados por una ejecución Gold."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from tlc_data_platform.audit.gold_dataset_repository import GoldDatasetRepository
from tlc_data_platform.audit.gold_execution_repository import GoldExecutionRepository
from tlc_data_platform.audit.processing_attempt_repository import ProcessingAttemptRepository


@dataclass(frozen=True)
class GoldAuditRepositories:
    executions: GoldExecutionRepository
    datasets: GoldDatasetRepository
    attempts: ProcessingAttemptRepository
    reconciliations: Any
    quality: Any
    unified: Any | None = None
