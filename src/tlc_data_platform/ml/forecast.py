from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from tlc_data_platform.core.settings import ForecastConfig
from tlc_data_platform.ml.common import baseline_metrics, metrics_frame, regression_metrics
from tlc_data_platform.ml.models import MLModelResult, utc_now

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ForecastOutput:
    result: MLModelResult
    predictions: Any
    anomalies: Any
    metrics: Any


NUMERIC_FEATURES = [
    "lag_1_hour",
    "lag_2_hours",
    "lag_24_hours",
    "lag_48_hours",
    "lag_168_hours",
    "rolling_mean_6h",
    "rolling_mean_24h",
    "rolling_mean_168h",
    "rolling_stddev_24h",
    "hour_of_day",
    "day_of_week",
    "month",
    "is_weekend_num",
    "is_peak_hour_num",
    "airport_zone_flag_num",
]
CATEGORICAL_FEATURES = ["service_type", "pickup_zone_code", "pickup_borough"]


def _prepare(frame: Any) -> Any:
    """Normalizes feature types once so every candidate algorithm sees the same input."""
    from pyspark.sql import functions as F

    prepared = (
        frame.withColumn("pickup_zone_code", F.col("pickup_location_id").cast("string"))
        .withColumn("pickup_borough", F.coalesce("pickup_borough", F.lit("UNKNOWN")))
        .withColumn("is_weekend_num", F.col("is_weekend").cast("double"))
        .withColumn("is_peak_hour_num", F.col("is_peak_hour").cast("double"))
        .withColumn("airport_zone_flag_num", F.coalesce(F.col("airport_zone_flag"), F.lit(False)).cast("double"))
    )
    return prepared.fillna(0.0, subset=NUMERIC_FEATURES)


def _pipeline(algorithm: str, config: ForecastConfig, seed: int) -> Any:
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
    from pyspark.ml.regression import GBTRegressor, GeneralizedLinearRegression, RandomForestRegressor

    indexers = [
        StringIndexer(inputCol=name, outputCol=f"{name}_index", handleInvalid="keep")
        for name in CATEGORICAL_FEATURES
    ]
    encoder = OneHotEncoder(
        inputCols=[f"{name}_index" for name in CATEGORICAL_FEATURES],
        outputCols=[f"{name}_ohe" for name in CATEGORICAL_FEATURES],
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=[*NUMERIC_FEATURES, *[f"{name}_ohe" for name in CATEGORICAL_FEATURES]],
        outputCol="features",
        handleInvalid="keep",
    )
    if algorithm == "gbt":
        estimator = GBTRegressor(
            labelCol="trip_count",
            featuresCol="features",
            predictionCol="prediction",
            maxIter=config.gbt_max_iter,
            maxDepth=config.gbt_max_depth,
            seed=seed,
        )
    elif algorithm == "random_forest":
        estimator = RandomForestRegressor(
            labelCol="trip_count",
            featuresCol="features",
            predictionCol="prediction",
            numTrees=config.random_forest_num_trees,
            maxDepth=config.random_forest_max_depth,
            seed=seed,
        )
    elif algorithm == "poisson_glr":
        estimator = GeneralizedLinearRegression(
            labelCol="trip_count",
            featuresCol="features",
            predictionCol="prediction",
            family="poisson",
            link="log",
            maxIter=60,
            regParam=0.05,
        )
    else:
        raise ValueError(f"Algoritmo de forecast no soportado: {algorithm}")
    return Pipeline(stages=[*indexers, encoder, assembler, estimator])


def _split(frame: Any, config: ForecastConfig) -> tuple[Any, Any, Any]:
    from pyspark.sql import functions as F

    train = frame.filter(
        (F.col("event_timestamp") >= F.to_timestamp(F.lit(config.train_start)))
        & (F.col("event_timestamp") <= F.to_timestamp(F.lit(config.train_end)))
    )
    validation = frame.filter(
        (F.col("event_timestamp") > F.to_timestamp(F.lit(config.train_end)))
        & (F.col("event_timestamp") <= F.to_timestamp(F.lit(config.validation_end)))
    )
    test = frame.filter(
        (F.col("event_timestamp") > F.to_timestamp(F.lit(config.validation_end)))
        & (F.col("event_timestamp") <= F.to_timestamp(F.lit(config.test_end)))
    )
    return train, validation, test


def _require_non_empty(frame: Any, name: str) -> int:
    rows = frame.count()
    if rows == 0:
        raise ValueError(f"El conjunto {name} del forecast está vacío; revisa ml.yml y Gold.")
    return rows


def _future_feature_frame(history: Any, horizon_hours: int) -> Any:
    """Builds direct future rows from the latest observed seasonal history.

    This is intentionally a global direct forecast, not 265 independent models.
    Exact 24/48/168-hour lags remain available for the whole horizon. Missing
    short lags are replaced by seasonal/rolling values, avoiding hundreds of
    expensive recursive Spark fits or one model per zone.
    """
    from pyspark.sql import functions as F

    spark = history.sparkSession
    latest = history.agg(F.max("event_timestamp").alias("latest")).first()["latest"]
    if latest is None:
        raise ValueError("No existe una última hora observada para generar el forecast.")

    active = history.groupBy(
        "pickup_location_id",
        "pickup_zone_name",
        "pickup_borough",
        "service_type",
        "airport_zone_flag",
    ).agg(
        F.count("event_timestamp").alias("active_hours"),
        F.sum("trip_count").alias("total_trips"),
    )
    horizon = spark.range(1, horizon_hours + 1).select(F.col("id").cast("int").alias("horizon_hour"))
    # Keep the latest value inside Spark. Converting a timezone-naive TLC
    # timestamp through Python's ``datetime.timestamp()`` would reinterpret it
    # using the host timezone and could shift the forecast by several hours.
    latest_frame = history.agg(F.max("event_timestamp").alias("latest_timestamp"))
    future = (
        active.crossJoin(horizon)
        .crossJoin(latest_frame)
        .withColumn(
            "event_timestamp",
            F.expr("timestampadd(HOUR, horizon_hour, latest_timestamp)"),
        )
        .drop("latest_timestamp")
    )

    history_values = history.select(
        "pickup_location_id", "service_type", "event_timestamp", "trip_count"
    )
    for hours, output_name in (
        (1, "lag_1_hour"),
        (2, "lag_2_hours"),
        (24, "lag_24_hours"),
        (48, "lag_48_hours"),
        (168, "lag_168_hours"),
    ):
        lagged = history_values.select(
            "pickup_location_id",
            "service_type",
            F.expr(f"event_timestamp + INTERVAL {hours} HOURS").alias("event_timestamp"),
            F.col("trip_count").alias(output_name),
        )
        future = future.join(
            lagged,
            ["pickup_location_id", "service_type", "event_timestamp"],
            "left",
        )

    # Python only subtracts timedeltas here; Spark still receives the original
    # local wall-clock values, so no UTC conversion is introduced.
    from datetime import timedelta

    cutoff_6h = latest - timedelta(hours=6)
    cutoff_24h = latest - timedelta(hours=24)
    cutoff_168h = latest - timedelta(hours=168)
    recent = history.filter(F.col("event_timestamp") > F.lit(cutoff_168h))
    rolling = recent.groupBy("pickup_location_id", "service_type").agg(
        F.avg(
            F.when(F.col("event_timestamp") > F.lit(cutoff_6h), F.col("trip_count"))
        ).alias("rolling_mean_6h"),
        F.avg(
            F.when(F.col("event_timestamp") > F.lit(cutoff_24h), F.col("trip_count"))
        ).alias("rolling_mean_24h"),
        F.avg("trip_count").alias("rolling_mean_168h"),
        F.stddev_pop(
            F.when(F.col("event_timestamp") > F.lit(cutoff_24h), F.col("trip_count"))
        ).alias("rolling_stddev_24h"),
    )
    future = future.join(rolling, ["pickup_location_id", "service_type"], "left")

    fallback = F.coalesce(
        "lag_24_hours", "lag_168_hours", "rolling_mean_168h", F.lit(0.0)
    )
    return (
        future.withColumn("lag_1_hour", F.coalesce("lag_1_hour", fallback))
        .withColumn("lag_2_hours", F.coalesce("lag_2_hours", fallback))
        .withColumn("lag_24_hours", F.coalesce("lag_24_hours", "lag_168_hours", "rolling_mean_168h", F.lit(0.0)))
        .withColumn("lag_48_hours", F.coalesce("lag_48_hours", "lag_168_hours", "rolling_mean_168h", F.lit(0.0)))
        .withColumn("lag_168_hours", F.coalesce("lag_168_hours", "rolling_mean_168h", F.lit(0.0)))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_hour", F.hour("event_timestamp"))
        .withColumn("hour_of_day", F.hour("event_timestamp"))
        .withColumn("day_of_week", F.dayofweek("event_timestamp"))
        .withColumn("month", F.month("event_timestamp"))
        .withColumn("is_weekend", F.dayofweek("event_timestamp").isin(1, 7))
        .withColumn("is_peak_hour", F.hour("event_timestamp").isin(7, 8, 9, 16, 17, 18, 19))
        .withColumn("same_hour_previous_day", F.col("lag_24_hours"))
        .withColumn("same_hour_previous_week", F.col("lag_168_hours"))
    )


def train_forecast(
    features: Any,
    config: ForecastConfig,
    *,
    seed: int,
    model_root: Path,
) -> ForecastOutput:
    """Trains and selects a global zone-hour-service demand model."""
    from pyspark.sql import functions as F

    active = features.groupBy("pickup_location_id", "service_type").agg(
        F.count("event_timestamp").alias("active_hours_filter"),
        F.sum("trip_count").alias("total_trips_filter"),
    ).filter(
        (F.col("active_hours_filter") >= config.minimum_active_hours)
        & (F.col("total_trips_filter") >= config.minimum_total_trips)
    )
    prepared = _prepare(
        features.join(active.select("pickup_location_id", "service_type"), ["pickup_location_id", "service_type"], "inner")
    ).persist()
    train, validation, test = _split(prepared, config)
    training_rows = _require_non_empty(train, "training")
    validation_rows = _require_non_empty(validation, "validation")
    test_rows = _require_non_empty(test, "test")

    candidates: list[tuple[str, Any, dict[str, float]]] = []
    for algorithm in config.algorithms:
        try:
            model = _pipeline(algorithm, config, seed).fit(train)
            metrics = regression_metrics(model.transform(validation), "trip_count")
            candidates.append((algorithm, model, metrics))
        except Exception as error:
            # One candidate may be numerically unsuitable (for example Poisson
            # convergence), but the comparison remains valid if other declared
            # candidates train successfully. The failure is logged, not hidden.
            LOGGER.exception("Forecast candidate %s failed: %s", algorithm, error)
    if not candidates:
        raise RuntimeError("Ningún algoritmo de forecast pudo entrenarse.")

    winner_algorithm, _, validation_metrics = min(
        candidates, key=lambda item: (item[2]["wape"], item[2]["rmse"])
    )
    train_plus_validation = train.unionByName(validation)
    final_model = _pipeline(winner_algorithm, config, seed).fit(train_plus_validation)
    test_predictions = (
        final_model.transform(test)
        .withColumn("prediction", F.greatest(F.col("prediction"), F.lit(0.0)))
        .persist()
    )
    test_metrics = regression_metrics(test_predictions, "trip_count")
    baseline_24 = baseline_metrics(test, "lag_24_hours", "trip_count")
    baseline_168 = baseline_metrics(test, "lag_168_hours", "trip_count")
    test_metrics.update(
        {
            "validation_wape": validation_metrics["wape"],
            "baseline_24h_wape": baseline_24["wape"],
            "baseline_168h_wape": baseline_168["wape"],
            "wape_improvement_vs_weekly": baseline_168["wape"] - test_metrics["wape"],
        }
    )

    model_id = str(uuid4())
    model_path = model_root / model_id
    model_path.parent.mkdir(parents=True, exist_ok=True)
    final_model.write().overwrite().save(str(model_path))

    residuals = test_predictions.withColumn(
        "residual", F.col("trip_count") - F.col("prediction")
    )
    global_std = residuals.agg(F.stddev_pop("residual").alias("std")).first()["std"] or 1.0
    residual_stats = residuals.groupBy("pickup_location_id", "service_type").agg(
        F.stddev_pop("residual").alias("residual_stddev")
    )
    anomalies = (
        residuals.join(residual_stats, ["pickup_location_id", "service_type"], "left")
        .withColumn(
            "deviation_score",
            F.col("residual") / F.coalesce(F.when(F.col("residual_stddev") > 0, F.col("residual_stddev")), F.lit(global_std)),
        )
        .withColumn("anomaly_flag", F.abs("deviation_score") >= config.anomaly_zscore_threshold)
        .withColumn(
            "anomaly_type",
            F.when(F.col("deviation_score") >= config.anomaly_zscore_threshold, "DEMAND_SURGE")
            .when(F.col("deviation_score") <= -config.anomaly_zscore_threshold, "DEMAND_DROP")
            .otherwise("NORMAL"),
        )
        .withColumn(
            "severity",
            F.when(F.abs("deviation_score") >= 5, "CRITICAL")
            .when(F.abs("deviation_score") >= 4, "HIGH")
            .when(F.abs("deviation_score") >= config.anomaly_zscore_threshold, "ELEVATED")
            .otherwise("NORMAL"),
        )
        .select(
            "event_timestamp",
            "pickup_location_id",
            "pickup_zone_name",
            "pickup_borough",
            "service_type",
            F.col("trip_count").alias("observed_trip_count"),
            F.col("prediction").alias("expected_trip_count"),
            "residual",
            "deviation_score",
            "anomaly_flag",
            "anomaly_type",
            "severity",
        )
    )

    future = _prepare(_future_feature_frame(prepared, config.forecast_horizon_hours))
    thresholds = prepared.groupBy("pickup_location_id", "service_type").agg(
        F.expr("percentile_approx(trip_count, array(0.50, 0.75, 0.90), 10000)").alias("demand_quantiles")
    )
    interval = test_predictions.select(
        F.abs(F.col("trip_count") - F.col("prediction")).alias("absolute_error")
    ).approxQuantile("absolute_error", [0.90], 0.01)[0]
    generated_at = utc_now()
    future_predictions = (
        final_model.transform(future)
        .withColumn("predicted_trip_count", F.greatest(F.col("prediction"), F.lit(0.0)))
        .join(thresholds, ["pickup_location_id", "service_type"], "left")
        .withColumn("lower_bound", F.greatest(F.col("predicted_trip_count") - F.lit(interval), F.lit(0.0)))
        .withColumn("upper_bound", F.col("predicted_trip_count") + F.lit(interval))
        .withColumn(
            "demand_level",
            F.when(F.col("predicted_trip_count") > F.element_at("demand_quantiles", 3), "CRITICAL")
            .when(F.col("predicted_trip_count") > F.element_at("demand_quantiles", 2), "HIGH")
            .when(F.col("predicted_trip_count") > F.element_at("demand_quantiles", 1), "NORMAL")
            .otherwise("LOW"),
        )
        .withColumn("model_id", F.lit(model_id))
        .withColumn("generated_at", F.lit(generated_at))
        .select(
            F.col("event_timestamp").alias("forecast_timestamp"),
            "horizon_hour",
            "pickup_location_id",
            "pickup_zone_name",
            "pickup_borough",
            "service_type",
            "predicted_trip_count",
            "lower_bound",
            "upper_bound",
            "demand_level",
            "model_id",
            "generated_at",
        )
    )

    metric_frame = metrics_frame(features.sparkSession, "forecast", winner_algorithm, test_metrics)
    result = MLModelResult(
        model_id=model_id,
        model_name="forecast",
        algorithm=winner_algorithm,
        status="SUCCESS",
        trained_at=generated_at,
        training_rows=training_rows,
        validation_rows=validation_rows,
        test_rows=test_rows,
        metrics=test_metrics,
        model_path=str(model_path),
        output_paths={},
        feature_columns=[*NUMERIC_FEATURES, *CATEGORICAL_FEATURES],
        target_column="trip_count",
    )
    test_predictions.unpersist()
    prepared.unpersist()
    return ForecastOutput(result, future_predictions, anomalies, metric_frame)
