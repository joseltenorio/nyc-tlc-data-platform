from __future__ import annotations

from tlc_data_platform.core.settings import AppConfig
from tlc_data_platform.ml.models import MLExecutionSummary, MLPlanSummary
from tlc_data_platform.ml.pipeline import MLPipeline


def plan_ml_pipeline(config: AppConfig, models: list[str] | None = None) -> MLPlanSummary:
    pipeline = MLPipeline(config)
    try:
        return pipeline.plan(models)
    finally:
        pipeline.close()


def run_ml_pipeline(config: AppConfig, models: list[str] | None = None) -> MLExecutionSummary:
    pipeline = MLPipeline(config)
    try:
        return pipeline.run(models)
    finally:
        pipeline.close()
