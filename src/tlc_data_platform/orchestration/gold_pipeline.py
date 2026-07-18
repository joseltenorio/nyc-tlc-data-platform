from __future__ import annotations

from collections.abc import Iterable

from tlc_data_platform.core.settings import AppConfig, RunSelection
from tlc_data_platform.gold.models import GoldExecutionSummary, GoldPlanSummary
from tlc_data_platform.gold.pipeline import GoldPipeline


def plan_gold_pipeline(config: AppConfig, selection: RunSelection) -> GoldPlanSummary:
    pipeline = GoldPipeline(config)
    try:
        return pipeline.plan(selection)
    finally:
        pipeline.close()


def run_gold_pipeline(
    config: AppConfig,
    selection: RunSelection,
    *,
    execution_type: str = "run",
    stages: Iterable[str] | None = None,
) -> GoldExecutionSummary:
    pipeline = GoldPipeline(config)
    try:
        return pipeline.run(selection, execution_type=execution_type, stages=stages)
    finally:
        pipeline.close()
