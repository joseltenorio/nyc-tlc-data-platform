"""Contiene métricas compartidas para regresión, clasificación y comparación con baselines."""

from __future__ import annotations

import math
from typing import Any


def regression_metrics(frame: Any, label_col: str = "trip_count") -> dict[str, float]:
    """Calculates scale-aware metrics without collecting all predictions."""
    from pyspark.sql import functions as F

    row = frame.agg(
        F.avg(F.abs(F.col("prediction") - F.col(label_col))).alias("mae"),
        F.sqrt(F.avg(F.pow(F.col("prediction") - F.col(label_col), 2))).alias("rmse"),
        (
            F.sum(F.abs(F.col("prediction") - F.col(label_col)))
            / F.greatest(F.sum(F.abs(F.col(label_col))), F.lit(1.0))
        ).alias("wape"),
    ).first()
    return {name: float(row[name] or 0.0) for name in ("mae", "rmse", "wape")}


def baseline_metrics(frame: Any, baseline_col: str, label_col: str = "trip_count") -> dict[str, float]:
    from pyspark.sql import functions as F

    baseline = frame.filter(F.col(baseline_col).isNotNull()).select(
        F.col(label_col), F.col(baseline_col).cast("double").alias("prediction")
    )
    if baseline.limit(1).count() == 0:
        return {"mae": math.inf, "rmse": math.inf, "wape": math.inf}
    return regression_metrics(baseline, label_col)


def classification_metrics(frame: Any, label_col: str) -> dict[str, float]:
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

    auc_pr = BinaryClassificationEvaluator(
        labelCol=label_col, rawPredictionCol="rawPrediction", metricName="areaUnderPR"
    ).evaluate(frame)
    auc_roc = BinaryClassificationEvaluator(
        labelCol=label_col, rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    ).evaluate(frame)
    metrics = {"auc_pr": float(auc_pr), "auc_roc": float(auc_roc)}
    for name, spark_name in (
        ("f1", "f1"),
        ("precision", "weightedPrecision"),
        ("recall", "weightedRecall"),
        ("accuracy", "accuracy"),
    ):
        metrics[name] = float(
            MulticlassClassificationEvaluator(
                labelCol=label_col, predictionCol="prediction", metricName=spark_name
            ).evaluate(frame)
        )
    return metrics


def metrics_frame(spark: Any, model_name: str, algorithm: str, metrics: dict[str, float]) -> Any:
    rows = [(model_name, algorithm, name, float(value)) for name, value in sorted(metrics.items())]
    return spark.createDataFrame(
        rows, "model_name string, algorithm string, metric_name string, metric_value double"
    )
