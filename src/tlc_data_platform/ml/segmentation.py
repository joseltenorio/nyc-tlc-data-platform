from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from tlc_data_platform.core.settings import SegmentationConfig
from tlc_data_platform.ml.common import metrics_frame
from tlc_data_platform.ml.models import MLModelResult, utc_now


@dataclass(frozen=True)
class SegmentationOutput:
    result: MLModelResult
    assignments: Any
    profiles: Any
    metrics: Any


FEATURE_COLUMNS = [
    "total_pickups",
    "total_dropoffs",
    "average_duration_seconds",
    "average_distance_miles",
    "average_speed_mph",
    "night_trip_share",
    "weekend_trip_share",
    "airport_trip_share",
    "yellow_share",
    "green_share",
    "fhv_share",
    "fhvhv_share",
    "service_diversity",
    "demand_coefficient_of_variation",
    "average_request_to_pickup_seconds",
    "shared_match_share",
    "wav_match_share",
]


def _pipeline(k: int, config: SegmentationConfig, seed: int) -> Any:
    from pyspark.ml import Pipeline
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.feature import StandardScaler, VectorAssembler

    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="raw_features",
        handleInvalid="keep",
    )
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withMean=True,
        withStd=True,
    )
    kmeans = KMeans(
        featuresCol="features",
        predictionCol="cluster_id",
        k=k,
        maxIter=config.max_iter,
        seed=seed,
    )
    return Pipeline(stages=[assembler, scaler, kmeans])


def _cluster_names(profile_rows: list[Any]) -> dict[int, str]:
    """Converts numeric clusters into business-friendly, unique labels."""
    if not profile_rows:
        return {}
    remaining = {int(row["cluster_id"]): row.asDict() for row in profile_rows}
    names: dict[int, str] = {}

    def assign(metric: str, label: str) -> None:
        candidates = [(cluster, data.get(metric) or 0.0) for cluster, data in remaining.items()]
        if not candidates:
            return
        cluster = max(candidates, key=lambda item: item[1])[0]
        names[cluster] = label
        remaining.pop(cluster, None)

    assign("airport_trip_share", "Zonas aeroportuarias")
    assign("total_pickups", "Núcleos de alta demanda")
    assign("night_trip_share", "Centros de actividad nocturna")
    assign("service_diversity", "Mercados multimodales")
    assign("weekend_trip_share", "Zonas recreativas de fin de semana")
    assign("average_request_to_pickup_seconds", "Zonas con fricción de recogida")

    fallback = [
        "Corredores residenciales estables",
        "Zonas emergentes",
        "Zonas de baja actividad",
        "Corredores de demanda variable",
    ]
    for index, cluster in enumerate(sorted(remaining)):
        names[cluster] = fallback[index] if index < len(fallback) else f"Perfil territorial {index + 1}"
    return names


def train_segmentation(
    features: Any,
    config: SegmentationConfig,
    *,
    seed: int,
    model_root: Path,
) -> SegmentationOutput:
    """Selects K by silhouette and segments TLC zones, not individual trips."""
    from pyspark.ml.evaluation import ClusteringEvaluator
    from pyspark.sql import functions as F

    prepared = features.fillna(0.0, subset=FEATURE_COLUMNS).filter(F.col("total_pickups") > 0).persist()
    rows = prepared.count()
    if rows < config.k_min:
        raise ValueError(
            f"Solo existen {rows} zonas activas; no se puede evaluar k={config.k_min}."
        )

    evaluator = ClusteringEvaluator(
        featuresCol="features", predictionCol="cluster_id", metricName="silhouette"
    )
    candidates: list[tuple[int, float, Any]] = []
    upper = min(config.k_max, rows - 1)
    for k in range(config.k_min, upper + 1):
        model = _pipeline(k, config, seed).fit(prepared)
        transformed = model.transform(prepared)
        silhouette = float(evaluator.evaluate(transformed))
        candidates.append((k, silhouette, model))
    if not candidates:
        raise ValueError("No fue posible evaluar ningún valor de k.")

    best_k, best_silhouette, best_model = max(candidates, key=lambda item: item[1])
    model_id = str(uuid4())
    model_path = model_root / model_id
    model_path.parent.mkdir(parents=True, exist_ok=True)
    best_model.write().overwrite().save(str(model_path))

    raw_assignments = best_model.transform(prepared).persist()
    profile_expressions = [F.avg(column).alias(column) for column in FEATURE_COLUMNS]
    profiles = raw_assignments.groupBy("cluster_id").agg(
        F.count("zone_key").alias("zone_count"), *profile_expressions
    )
    names = _cluster_names(profiles.collect())
    name_rows = [(cluster, label) for cluster, label in sorted(names.items())]
    name_frame = features.sparkSession.createDataFrame(name_rows, "cluster_id int, segment_name string")
    profiles = profiles.join(name_frame, "cluster_id", "left")
    assignments = raw_assignments.join(name_frame, "cluster_id", "left").select(
        "zone_key",
        "location_id",
        "zone_name",
        "borough",
        "service_zone",
        "is_airport",
        "cluster_id",
        "segment_name",
        *FEATURE_COLUMNS,
    )

    metrics = {
        "best_k": float(best_k),
        "silhouette": best_silhouette,
        **{f"silhouette_k_{k}": score for k, score, _ in candidates},
    }
    metric_frame = metrics_frame(features.sparkSession, "segmentation", "kmeans", metrics)
    trained_at = utc_now()
    result = MLModelResult(
        model_id=model_id,
        model_name="segmentation",
        algorithm="kmeans",
        status="SUCCESS",
        trained_at=trained_at,
        training_rows=rows,
        validation_rows=0,
        test_rows=0,
        metrics=metrics,
        model_path=str(model_path),
        output_paths={},
        feature_columns=FEATURE_COLUMNS,
        target_column="cluster_id",
    )
    raw_assignments.unpersist()
    prepared.unpersist()
    return SegmentationOutput(result, assignments, profiles, metric_frame)
