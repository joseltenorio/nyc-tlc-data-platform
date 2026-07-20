from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from tlc_data_platform.bronze.models import ExecutionSummary
from tlc_data_platform.core.settings import AppConfig, RunSelection
from tlc_data_platform.orchestration.bronze_pipeline import run_bronze_pipeline
from tlc_data_platform.orchestration.silver_pipeline import run_silver_pipeline
from tlc_data_platform.silver.models import SilverExecutionSummary


@dataclass(frozen=True)
class MedallionExecutionSummary:
    status: str
    bronze: ExecutionSummary
    silver: SilverExecutionSummary | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "bronze": self.bronze.to_dict(),
            "silver": self.silver.to_dict() if self.silver else None,
        }


def run_medallion_to_silver(
    config: AppConfig,
    selection: RunSelection,
    *,
    execution_type: str,
    force_bronze: bool = False,
    force_silver: bool = False,
    refresh_references: bool | None = None,
) -> MedallionExecutionSummary:
    bronze = run_bronze_pipeline(
        config,
        selection,
        execution_type=execution_type,
        force=force_bronze,
    )
    # Silver must only start after a complete Bronze execution. A
    # PARTIAL_SUCCESS can contain missing, deferred or claimed periods and is
    # therefore not a valid input boundary for the next Medallion layer.
    if bronze.status != "SUCCESS":
        return MedallionExecutionSummary(bronze.status, bronze, None)
    silver = run_silver_pipeline(
        config,
        selection,
        execution_type=execution_type,
        force=force_silver,
        refresh_references=refresh_references,
    )
    if bronze.status == "PARTIAL_SUCCESS" or silver.status == "PARTIAL_SUCCESS":
        status = "PARTIAL_SUCCESS"
    elif silver.status in {"SUCCESS", "NO_INPUT"}:
        status = "SUCCESS"
    else:
        status = "FAILED"
    return MedallionExecutionSummary(status, bronze, silver)