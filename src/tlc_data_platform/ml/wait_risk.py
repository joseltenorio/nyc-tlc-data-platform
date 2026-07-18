from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from tlc_data_platform.core.settings import WaitRiskConfig
from tlc_data_platform.ml.common import classification_metrics, metrics_frame
from tlc_data_platform.ml.models import MLModelResult, utc_now

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class WaitRiskOutput:
    result: MLModelResult
    predictions: Any
    metrics: Any
    feature_importance: Any


NUMERIC_FEATURES = [
    "request_hour",
    "request_day_of_week",
    "request_month",
    "is_weekend_num",
    "is_peak_hour_num",
    "airport_zone_flag_num",
    "shared_requested_num",
    "access_a_ride_num",
    "wav_requested_num",
]
CATEGORICAL_FEATURES = ["pickup_zone_code", "pickup_borough", "hvfhs_license_num"]
LABEL_COLUMN = "excessive_wait_flag"


def _prepare(frame: Any, threshold_seconds: int) -> Any:
    """Creates the binary target while keeping post-request outcomes out of features."""
    from pyspark.sql import functions as F

    return (
        frame.withColumn(
            LABEL_COLUMN,
            (F.col("request_to_pickup_seconds") >= threshold_seconds).cast("double"),
        )
        .withColumn("pickup_zone_code", F.col("pickup_zone_key").cast("string"))
        .withColumn("pickup_borough", F.coalesce("pickup_borough", F.lit("UNKNOWN")))
        .withColumn("hvfhs_license_num", F.coalesce("hvfhs_license_num", F.lit("UNKNOWN")))
        .withColumn("is_weekend_num", F.col("is_weekend").cast("double"))
        .withColumn("is_peak_hour_num", F.col("is_peak_hour").cast("double"))
        .withColumn("airport_zone_flag_num", F.coalesce("airport_zone_flag", F.lit(False)).cast("double"))
        .withColumn("shared_requested_num", F.coalesce("shared_requested", F.lit(False)).cast("double"))
        .withColumn("access_a_ride_num", F.coalesce("access_a_ride", F.lit(False)).cast("double"))
        .withColumn("wav_requested_num", F.coalesce("wav_requested", F.lit(False)).cast("double"))
        .fillna(0.0, subset=NUMERIC_FEATURES)
    )


def _add_class_weights(frame: Any) -> Any:
    """Balances the loss without duplicating minority-class rows."""
    from pyspark.sql import functions as F

    counts = {float(row[LABEL_COLUMN]): int(row["count"]) for row in frame.groupBy(LABEL_COLUMN).count().collect()}
    negative = counts.get(0.0, 0)
    positive = counts.get(1.0, 0)
    if negative == 0 or positive == 0:
        raise ValueError("El entrenamiento de wait-risk necesita ejemplos de ambas clases.")
    total = negative + positive
    negative_weight = total / (2.0 * negative)
    positive_weight = total / (2.0 * positive)
    return frame.withColumn(
        "class_weight",
        F.when(F.col(LABEL_COLUMN) == 1.0, F.lit(positive_weight)).otherwise(F.lit(negative_weight)),
    )


def _pipeline(algorithm: str, config: WaitRiskConfig, seed: int) -> Any:
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
    from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

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
    common = {
        "labelCol": LABEL_COLUMN,
        "featuresCol": "features",
        "weightCol": "class_weight",
        "predictionCol": "prediction",
        "probabilityCol": "probability",
        "rawPredictionCol": "rawPrediction",
    }
    if algorithm == "logistic_regression":
        estimator = LogisticRegression(
            **common,
            maxIter=config.logistic_max_iter,
            regParam=0.05,
            elasticNetParam=0.0,
        )
    elif algorithm == "random_forest":
        estimator = RandomForestClassifier(
            **common,
            numTrees=config.random_forest_num_trees,
            maxDepth=config.random_forest_max_depth,
            seed=seed,
        )
    elif algorithm == "gbt":
        estimator = GBTClassifier(
            **common,
            maxIter=config.gbt_max_iter,
            maxDepth=config.gbt_max_depth,
            seed=seed,
        )
    else:
        raise ValueError(f"Algoritmo wait-risk no soportado: {algorithm}")
    return Pipeline(stages=[*indexers, encoder, assembler, estimator])


def _split(frame: Any, config: WaitRiskConfig) -> tuple[Any, Any, Any]:
    from pyspark.sql import functions as F

    train = frame.filter(F.col("request_datetime") <= F.to_timestamp(F.lit(config.train_end)))
    validation = frame.filter(
        (F.col("request_datetime") > F.to_timestamp(F.lit(config.train_end)))
        & (F.col("request_datetime") <= F.to_timestamp(F.lit(config.validation_end)))
    )
    test = frame.filter(
        (F.col("request_datetime") > F.to_timestamp(F.lit(config.validation_end)))
        & (F.col("request_datetime") <= F.to_timestamp(F.lit(config.test_end)))
    )
    return train, validation, test


def _feature_names(frame: Any) -> list[str]:
    metadata = frame.schema["features"].metadata.get("ml_attr", {}).get("attrs", {})
    attrs: list[dict[str, Any]] = []
    for values in metadata.values():
        attrs.extend(values)
    if not attrs:
        size = frame.select("features").first()["features"].size
        return [f"feature_{index}" for index in range(size)]
    return [item["name"] for item in sorted(attrs, key=lambda item: item["idx"])]


def _importance_frame(predictions: Any, model: Any, algorithm: str) -> Any:
    from pyspark.sql import functions as F

    names = _feature_names(predictions)
    estimator_model = model.stages[-1]
    if hasattr(estimator_model, "featureImportances"):
        values = estimator_model.featureImportances.toArray().tolist()
    elif hasattr(estimator_model, "coefficients"):
        values = [abs(float(value)) for value in estimator_model.coefficients.toArray().tolist()]
    else:
        values = [0.0] * len(names)
    rows = [(name, float(values[index]) if index < len(values) else 0.0) for index, name in enumerate(names)]
    return predictions.sparkSession.createDataFrame(
        rows, "feature_name string, importance double"
    ).orderBy(F.desc("importance"))


def train_wait_risk(
    features: Any,
    config: WaitRiskConfig,
    *,
    seed: int,
    model_root: Path,
) -> WaitRiskOutput:
    """Classifies excessive request-to-pickup wait for HVFHV trips.

    This replaces the high-tip classifier. It is better aligned with the traffic
    case and uses HVFHV's request timestamp, which taxi datasets do not provide.
    """
    from pyspark.ml.functions import vector_to_array
    from pyspark.sql import functions as F

    prepared = _prepare(features, config.excessive_wait_threshold_seconds).persist()
    train, validation, test = _split(prepared, config)
    train = _add_class_weights(train).persist()
    validation = _add_class_weights(validation).persist()
    test = _add_class_weights(test).persist()
    training_rows = train.count()
    validation_rows = validation.count()
    test_rows = test.count()
    if min(training_rows, validation_rows, test_rows) == 0:
        raise ValueError("Los cortes temporales de wait-risk no pueden estar vacíos.")

    candidates: list[tuple[str, Any, dict[str, float]]] = []
    for algorithm in config.algorithms:
        try:
            model = _pipeline(algorithm, config, seed).fit(train)
            metrics = classification_metrics(model.transform(validation), LABEL_COLUMN)
            candidates.append((algorithm, model, metrics))
        except Exception as error:
            LOGGER.exception("Wait-risk candidate %s failed: %s", algorithm, error)
    if not candidates:
        raise RuntimeError("Ningún clasificador de wait-risk pudo entrenarse.")

    winner_algorithm, _, validation_metrics = max(
        candidates, key=lambda item: (item[2]["auc_pr"], item[2]["f1"])
    )
    final_training = _add_class_weights(train.drop("class_weight").unionByName(validation.drop("class_weight")))
    final_model = _pipeline(winner_algorithm, config, seed).fit(final_training)
    raw_predictions = final_model.transform(test).persist()
    test_metrics = classification_metrics(raw_predictions, LABEL_COLUMN)
    test_metrics.update(
        {
            "validation_auc_pr": validation_metrics["auc_pr"],
            "validation_f1": validation_metrics["f1"],
            "excessive_wait_threshold_seconds": float(config.excessive_wait_threshold_seconds),
            "positive_rate": float(test.agg(F.avg(LABEL_COLUMN).alias("rate")).first()["rate"] or 0.0),
        }
    )

    model_id = str(uuid4())
    model_path = model_root / model_id
    model_path.parent.mkdir(parents=True, exist_ok=True)
    final_model.write().overwrite().save(str(model_path))
    trained_at = utc_now()

    predictions = (
        raw_predictions.withColumn("risk_probability", vector_to_array("probability")[1])
        .withColumn(
            "predicted_risk_level",
            F.when(F.col("risk_probability") >= 0.80, "CRITICAL")
            .when(F.col("risk_probability") >= 0.60, "HIGH")
            .when(F.col("risk_probability") >= 0.40, "ELEVATED")
            .otherwise("NORMAL"),
        )
        .withColumn("model_id", F.lit(model_id))
        .withColumn("generated_at", F.lit(trained_at))
        .select(
            "trip_key",
            "request_datetime",
            "pickup_zone_key",
            "pickup_zone_name",
            "pickup_borough",
            "hvfhs_license_num",
            "hvfhs_company_name",
            "request_to_pickup_seconds",
            F.col(LABEL_COLUMN).cast("int").alias("observed_excessive_wait"),
            F.col("prediction").cast("int").alias("predicted_excessive_wait"),
            "risk_probability",
            "predicted_risk_level",
            "model_id",
            "generated_at",
        )
    )
    importance = _importance_frame(raw_predictions, final_model, winner_algorithm).withColumn(
        "model_id", F.lit(model_id)
    )
    metric_frame = metrics_frame(features.sparkSession, "wait-risk", winner_algorithm, test_metrics)
    result = MLModelResult(
        model_id=model_id,
        model_name="wait-risk",
        algorithm=winner_algorithm,
        status="SUCCESS",
        trained_at=trained_at,
        training_rows=training_rows,
        validation_rows=validation_rows,
        test_rows=test_rows,
        metrics=test_metrics,
        model_path=str(model_path),
        output_paths={},
        feature_columns=[*NUMERIC_FEATURES, *CATEGORICAL_FEATURES],
        target_column=LABEL_COLUMN,
    )

    raw_predictions.unpersist()
    train.unpersist()
    validation.unpersist()
    test.unpersist()
    prepared.unpersist()
    return WaitRiskOutput(result, predictions, metric_frame, importance)
