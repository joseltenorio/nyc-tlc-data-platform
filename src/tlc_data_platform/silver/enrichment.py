from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from tlc_data_platform.core.settings import SilverStorageConfig
from tlc_data_platform.silver.transformers.common import append_quality_rules


@dataclass(frozen=True)
class SilverReferenceData:
    taxi_zones: Any
    base_lookup: Any

    @classmethod
    def load(cls, spark: Any, storage: SilverStorageConfig) -> "SilverReferenceData":
        taxi_path = storage.silver_root / storage.taxi_zones_dataset
        base_path = storage.silver_root / storage.base_lookup_dataset
        return cls(
            taxi_zones=spark.read.parquet(str(taxi_path)),
            base_lookup=spark.read.parquet(str(base_path)),
        )


def _add_null_columns(df: Any, columns: dict[str, str]) -> Any:
    from pyspark.sql import functions as F

    for name, dtype in columns.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(dtype))
    return df


def enrich_trip(
    df: Any,
    service: str,
    references: SilverReferenceData | None,
) -> Any:
    from pyspark.sql import functions as F

    zone_columns = {
        "pickup_zone_name": "string",
        "pickup_borough": "string",
        "pickup_service_zone": "string",
        "dropoff_zone_name": "string",
        "dropoff_borough": "string",
        "dropoff_service_zone": "string",
    }
    base_columns = {
        "dispatching_base_name": "string",
        "dispatching_base_dba": "string",
        "dispatching_base_type": "string",
        "dispatching_base_status": "string",
        "originating_base_name": "string",
        "affiliated_base_name": "string",
    }
    if references is None:
        return _add_null_columns(_add_null_columns(df, zone_columns), base_columns)

    zones = references.taxi_zones
    pickup = F.broadcast(
        zones.select(
            F.col("location_id").alias("pickup_location_id"),
            F.col("zone_name").alias("pickup_zone_name"),
            F.col("borough").alias("pickup_borough"),
            F.col("service_zone").alias("pickup_service_zone"),
        )
    )
    dropoff = F.broadcast(
        zones.select(
            F.col("location_id").alias("dropoff_location_id"),
            F.col("zone_name").alias("dropoff_zone_name"),
            F.col("borough").alias("dropoff_borough"),
            F.col("service_zone").alias("dropoff_service_zone"),
        )
    )
    df = df.join(pickup, "pickup_location_id", "left").join(
        dropoff, "dropoff_location_id", "left"
    )
    df = append_quality_rules(
        df,
        error_conditions={
            "PICKUP_ZONE_NOT_IN_LOOKUP": F.col("pickup_location_id").isNotNull()
            & F.col("pickup_zone_name").isNull(),
            "DROPOFF_ZONE_NOT_IN_LOOKUP": F.col("dropoff_location_id").isNotNull()
            & F.col("dropoff_zone_name").isNull(),
        },
    )

    if service not in {"fhv", "fhvhv"}:
        return _add_null_columns(df, base_columns)

    bases = references.base_lookup

    def join_base(
        frame: Any,
        source_column: str,
        prefix: str,
        *,
        include_details: bool,
    ) -> Any:
        if source_column not in frame.columns:
            return frame
        selections = [
            F.col("base_license_number").alias(source_column),
            F.col("base_name").alias(f"{prefix}_name"),
        ]
        if include_details:
            selections.extend(
                [
                    F.col("doing_business_as").alias(f"{prefix}_dba"),
                    F.col("base_type").alias(f"{prefix}_type"),
                    F.col("status").alias(f"{prefix}_status"),
                ]
            )
        return frame.join(F.broadcast(bases.select(*selections)), source_column, "left")

    df = join_base(
        df,
        "dispatching_base_num",
        "dispatching_base",
        include_details=True,
    )
    df = join_base(
        df,
        "originating_base_num",
        "originating_base",
        include_details=False,
    )
    df = join_base(
        df,
        "affiliated_base_number",
        "affiliated_base",
        include_details=False,
    )
    df = _add_null_columns(df, base_columns)

    warnings: dict[str, Any] = {}
    if "dispatching_base_num" in df.columns:
        warnings["DISPATCHING_BASE_NOT_IN_CURRENT_LOOKUP"] = (
            F.col("dispatching_base_num").isNotNull()
            & F.col("dispatching_base_name").isNull()
        )
    if "originating_base_num" in df.columns:
        warnings["ORIGINATING_BASE_NOT_IN_CURRENT_LOOKUP"] = (
            F.col("originating_base_num").isNotNull()
            & F.col("originating_base_name").isNull()
        )
    if "affiliated_base_number" in df.columns:
        warnings["AFFILIATED_BASE_NOT_IN_CURRENT_LOOKUP"] = (
            F.col("affiliated_base_number").isNotNull()
            & F.col("affiliated_base_name").isNull()
        )
    return append_quality_rules(df, warning_conditions=warnings)
