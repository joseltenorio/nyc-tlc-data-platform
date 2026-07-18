from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from tlc_data_platform.audit.unified import UnifiedAuditRepository, utc_now
from tlc_data_platform.core.settings import AppConfig, RunSelection
from tlc_data_platform.gold.models import GoldExecutionSummary
from tlc_data_platform.ml.models import MLExecutionSummary
from tlc_data_platform.mongodb.client import MongoClientProvider
from tlc_data_platform.orchestration.gold_pipeline import run_gold_pipeline
from tlc_data_platform.orchestration.medallion_pipeline import (
    MedallionExecutionSummary,
    run_medallion_to_silver,
)
from tlc_data_platform.orchestration.ml_pipeline import run_ml_pipeline


@dataclass(frozen=True)
class PlatformExecutionSummary:
    execution_id: str
    status: str
    bronze_silver: MedallionExecutionSummary
    gold: GoldExecutionSummary | None
    ml: MLExecutionSummary | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "status": self.status,
            "bronze_silver": self.bronze_silver.to_dict(),
            "gold": self.gold.to_dict() if self.gold else None,
            "ml": self.ml.to_dict() if self.ml else None,
        }


def run_platform_pipeline(
    config: AppConfig,
    selection: RunSelection,
    *,
    execution_type: str,
    force_bronze: bool = False,
    force_silver: bool = False,
    refresh_references: bool | None = None,
    train_ml: bool = True,
    models: list[str] | None = None,
) -> PlatformExecutionSummary:
    """Runs Bronze -> Silver -> Gold -> optional ML and audits the parent run."""
    execution_id = f"platform-{utc_now().strftime('%Y%m%dT%H%M%S')}-{uuid4().hex[:8]}"
    started_at = utc_now()
    mongo = MongoClientProvider(config.mongo)
    unified: UnifiedAuditRepository | None = None
    try:
        unified = UnifiedAuditRepository(mongo.database(), config.audit)
        unified.start_run(
            execution_id,
            layer="platform",
            execution_type=execution_type,
            selection={
                "services": selection.services,
                "start_year": selection.start_year,
                "end_year": selection.end_year,
                "months": selection.months,
                "train_ml": train_ml,
                "models": models,
            },
            started_at=started_at,
        )

        medallion = run_medallion_to_silver(
            config,
            selection,
            execution_type=execution_type,
            force_bronze=force_bronze,
            force_silver=force_silver,
            refresh_references=refresh_references,
        )
        unified.link_parent(medallion.bronze.execution_id, execution_id)
        if medallion.silver is not None:
            unified.link_parent(medallion.silver.execution_id, execution_id)
        if medallion.status == "FAILED":
            summary = PlatformExecutionSummary(execution_id, "FAILED", medallion, None, None)
            unified.finish_run(
                execution_id,
                status="FAILED",
                metrics={"completed_layers": 2, "failed_layer": "bronze_or_silver"},
            )
            return summary

        gold = run_gold_pipeline(
            config,
            selection,
            execution_type=execution_type,
            stages=("dimensions", "facts", "marts", "features"),
        )
        unified.link_parent(gold.execution_id, execution_id)
        if gold.status not in {"SUCCESS", "PARTIAL_SUCCESS", "NO_INPUT"}:
            summary = PlatformExecutionSummary(execution_id, "FAILED", medallion, gold, None)
            unified.finish_run(
                execution_id,
                status="FAILED",
                metrics={"completed_layers": 3, "failed_layer": "gold"},
            )
            return summary

        ml = (
            run_ml_pipeline(config, models)
            if train_ml and gold.status in {"SUCCESS", "PARTIAL_SUCCESS"}
            else None
        )
        if ml is not None:
            unified.link_parent(ml.run_id, execution_id)
        statuses = [medallion.status, gold.status, ml.status if ml else "SUCCESS"]
        if "FAILED" in statuses:
            status = "FAILED"
        elif "PARTIAL_SUCCESS" in statuses or "NO_INPUT" in statuses:
            status = "PARTIAL_SUCCESS"
        else:
            status = "SUCCESS"
        summary = PlatformExecutionSummary(execution_id, status, medallion, gold, ml)
        unified.finish_run(
            execution_id,
            status=status,
            metrics={
                "completed_layers": 4 if ml else 3,
                "bronze_execution_id": medallion.bronze.execution_id,
                "silver_execution_id": medallion.silver.execution_id if medallion.silver else None,
                "gold_execution_id": gold.execution_id,
                "ml_run_id": ml.run_id if ml else None,
                "bronze_silver_status": medallion.status,
                "gold_status": gold.status,
                "ml_status": ml.status if ml else "SKIPPED",
            },
        )
        return summary
    except Exception as error:
        if unified is not None:
            unified.fail_run(execution_id, error, layer="platform")
        raise
    finally:
        mongo.close()
