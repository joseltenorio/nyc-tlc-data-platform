from __future__ import annotations

from tlc_data_platform.core.settings import AppConfig, RunSelection
from tlc_data_platform.silver.models import (
    ReferenceRefreshSummary,
    SilverExecutionSummary,
    SilverPlanSummary,
)
from tlc_data_platform.silver.pipeline import SilverPipeline
from tlc_data_platform.silver.references import SilverReferencePipeline


def plan_silver_pipeline(config: AppConfig, selection: RunSelection) -> SilverPlanSummary:
    pipeline = SilverPipeline(config)
    try:
        return pipeline.plan(selection)
    finally:
        pipeline.close()


def run_silver_pipeline(
    config: AppConfig,
    selection: RunSelection,
    *,
    execution_type: str = "RUN",
    force: bool = False,
    refresh_references: bool | None = None,
) -> SilverExecutionSummary:
    pipeline = SilverPipeline(config)
    try:
        return pipeline.run(
            selection,
            execution_type=execution_type,
            force=force,
            refresh_references=refresh_references,
        )
    finally:
        pipeline.close()


def refresh_silver_references(config: AppConfig) -> ReferenceRefreshSummary:
    pipeline = SilverReferencePipeline(config)
    try:
        return pipeline.run()
    finally:
        pipeline.close()
