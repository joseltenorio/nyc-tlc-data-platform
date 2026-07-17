from __future__ import annotations

from typing import Any

from tlc_data_platform.core.settings import SilverQualityConfig
from tlc_data_platform.silver.models import SilverTransformContext
from tlc_data_platform.silver.transformers.common import (
    Rule,
    add_common_metadata,
    add_trip_business_id,
    apply_rules,
    col_from_aliases,
    common_rules,
    normalize_string,
)


def transform(raw: Any, context: SilverTransformContext, quality: SilverQualityConfig) -> Any:
    from pyspark.sql import functions as F

    sr_raw = normalize_string(col_from_aliases(raw, ["SR_Flag", "sr_flag"], "string"))
    df = raw.select(
        F.upper(
            F.trim(col_from_aliases(raw, ["dispatching_base_num"], "string"))
        ).alias("dispatching_base_num"),
        col_from_aliases(raw, ["pickup_datetime"], "timestamp_ntz").alias(
            "pickup_datetime"
        ),
        col_from_aliases(
            raw, ["dropOff_datetime", "dropoff_datetime"], "timestamp_ntz"
        ).alias("dropoff_datetime"),
        col_from_aliases(
            raw, ["PUlocationID", "PULocationID", "pulocationid"], "int"
        ).alias("pickup_location_id"),
        col_from_aliases(
            raw, ["DOlocationID", "DOLocationID", "dolocationid"], "int"
        ).alias("dropoff_location_id"),
        sr_raw.alias("shared_ride_flag_raw"),
        F.upper(
            F.trim(
                col_from_aliases(
                    raw,
                    ["Affiliated_base_number", "affiliated_base_number"],
                    "string",
                )
            )
        ).alias("affiliated_base_number"),
    )
    df = (
        df.withColumn(
            "shared_ride_flag",
            F.when(F.col("shared_ride_flag_raw").isin("1", "1.0"), F.lit(True))
            .when(
                F.col("shared_ride_flag_raw").isNull()
                | F.col("shared_ride_flag_raw").isin("0", "0.0"),
                F.lit(False),
            )
            .otherwise(F.lit(None).cast("boolean")),
        )
        .withColumn("shared_matched", F.col("shared_ride_flag"))
        .withColumn(
            "trip_duration_seconds",
            F.timestamp_diff(
                "SECOND", F.col("pickup_datetime"), F.col("dropoff_datetime")
            ).cast("long"),
        )
        .withColumn(
            "airport_trip_flag",
            F.col("pickup_location_id").isin(1, 132, 138)
            | F.col("dropoff_location_id").isin(1, 132, 138),
        )
    )
    df = add_common_metadata(df, context)
    df = add_trip_business_id(
        df,
        [
            "service_type",
            "dispatching_base_num",
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_location_id",
            "dropoff_location_id",
        ],
    )
    rules = common_rules(df, context, quality, quality.fhv_max_duration_hours)
    rules.extend(
        [
            Rule(
                "MISSING_DISPATCHING_BASE",
                "ERROR",
                F.col("dispatching_base_num").isNull()
                | (F.length("dispatching_base_num") == 0),
            ),
            Rule(
                "INVALID_SHARED_RIDE_FLAG",
                "WARNING",
                F.col("shared_ride_flag_raw").isNotNull()
                & ~F.col("shared_ride_flag_raw").isin("0", "0.0", "1", "1.0"),
            ),
        ]
    )
    return apply_rules(df, rules).drop("shared_ride_flag_raw")
