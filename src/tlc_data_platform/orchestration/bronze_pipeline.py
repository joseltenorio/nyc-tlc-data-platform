from __future__ import annotations

from tlc_data_platform.bronze.models import ExecutionSummary, PlanSummary
from tlc_data_platform.bronze.pipeline import BronzePipeline
from tlc_data_platform.core.settings import AppConfig, RunSelection


def plan_bronze_pipeline(
    config: AppConfig,
    selection: RunSelection,
) -> PlanSummary:
    pipeline = BronzePipeline(config)
    try:
        return pipeline.plan(selection)
    finally:
        pipeline.close()


def run_bronze_pipeline(
    config: AppConfig,
    selection: RunSelection,
    *,
    execution_type: str = "RUN",
    dry_run: bool = False,
    force: bool = False,
) -> ExecutionSummary:
    pipeline = BronzePipeline(config)
    try:
        return pipeline.run(
            selection,
            execution_type=execution_type,
            dry_run=dry_run,
            force=force,
        )
    finally:
        pipeline.close()
