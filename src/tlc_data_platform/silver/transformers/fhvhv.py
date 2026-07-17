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
    normalize_boolean,
    normalize_string,
)


HVFHS_COMPANIES = {
    "HV0002": "Juno",
    "HV0003": "Uber",
    "HV0004": "Via",
    "HV0005": "Lyft",
}


def transform(raw: Any, context: SilverTransformContext, quality: SilverQualityConfig) -> Any:
    from pyspark.sql import functions as F

    money = {
        "base_passenger_fare": ["base_passenger_fare"],
        "tolls": ["tolls"],
        "black_car_fund": ["bcf"],
        "sales_tax": ["sales_tax"],
        "congestion_surcharge": ["congestion_surcharge"],
        "airport_fee": ["airport_fee"],
        "tips": ["tips"],
        "driver_pay": ["driver_pay"],
        "cbd_congestion_fee": ["cbd_congestion_fee"],
    }
    flag_aliases = {
        "shared_request_flag": ["shared_request_flag"],
        "shared_match_flag": ["shared_match_flag"],
        "access_a_ride_flag": ["access_a_ride_flag"],
        "wav_request_flag": ["wav_request_flag"],
        "wav_match_flag": ["wav_match_flag"],
    }
    raw_flags = {
        name: normalize_string(col_from_aliases(raw, aliases, "string"))
        for name, aliases in flag_aliases.items()
    }
    df = raw.select(
        F.upper(F.trim(col_from_aliases(raw, ["hvfhs_license_num"], "string"))).alias(
            "hvfhs_license_num"
        ),
        F.upper(
            F.trim(col_from_aliases(raw, ["dispatching_base_num"], "string"))
        ).alias("dispatching_base_num"),
        F.upper(
            F.trim(col_from_aliases(raw, ["originating_base_num"], "string"))
        ).alias("originating_base_num"),
        col_from_aliases(raw, ["request_datetime"], "timestamp_ntz").alias(
            "request_datetime"
        ),
        col_from_aliases(raw, ["on_scene_datetime"], "timestamp_ntz").alias(
            "on_scene_datetime"
        ),
        col_from_aliases(raw, ["pickup_datetime"], "timestamp_ntz").alias(
            "pickup_datetime"
        ),
        col_from_aliases(raw, ["dropoff_datetime"], "timestamp_ntz").alias(
            "dropoff_datetime"
        ),
        col_from_aliases(raw, ["PULocationID", "pulocationid"], "int").alias(
            "pickup_location_id"
        ),
        col_from_aliases(raw, ["DOLocationID", "dolocationid"], "int").alias(
            "dropoff_location_id"
        ),
        col_from_aliases(raw, ["trip_miles"], "double").alias("trip_miles"),
        col_from_aliases(raw, ["trip_time"], "long").alias("trip_time"),
        *[
            col_from_aliases(raw, aliases, "double", 0.0).alias(name)
            for name, aliases in money.items()
        ],
        *[column.alias(f"{name}_raw") for name, column in raw_flags.items()],
    )
    for name in money:
        df = df.withColumn(name, F.coalesce(F.col(name), F.lit(0.0)).cast("double"))
    for name in flag_aliases:
        df = df.withColumn(
            name,
            F.when(
                F.col(f"{name}_raw").isin(*quality.allowed_boolean_flags),
                F.col(f"{name}_raw"),
            ).otherwise(F.lit(None).cast("string")),
        )

    company_expr = F.create_map(
        *[
            item
            for code, company in HVFHS_COMPANIES.items()
            for item in (F.lit(code), F.lit(company))
        ]
    )
    df = (
        df.withColumn("hvfhs_company_name", company_expr[F.col("hvfhs_license_num")])
        .withColumn("shared_requested", normalize_boolean(F.col("shared_request_flag")))
        .withColumn("shared_matched", normalize_boolean(F.col("shared_match_flag")))
        .withColumn("access_a_ride", normalize_boolean(F.col("access_a_ride_flag")))
        .withColumn("wav_requested", normalize_boolean(F.col("wav_request_flag")))
        .withColumn("wav_matched", normalize_boolean(F.col("wav_match_flag")))
        .withColumn(
            "trip_duration_seconds",
            F.timestamp_diff(
                "SECOND", F.col("pickup_datetime"), F.col("dropoff_datetime")
            ).cast("long"),
        )
        .withColumn(
            "request_to_pickup_seconds",
            F.timestamp_diff(
                "SECOND", F.col("request_datetime"), F.col("pickup_datetime")
            ).cast("long"),
        )
        .withColumn(
            "driver_wait_seconds",
            F.when(
                F.col("on_scene_datetime").isNotNull(),
                F.timestamp_diff(
                    "SECOND", F.col("on_scene_datetime"), F.col("pickup_datetime")
                ),
            ).cast("long"),
        )
        .withColumn(
            "average_speed_mph",
            F.when(
                (F.col("trip_duration_seconds") > 0) & (F.col("trip_miles") >= 0),
                F.col("trip_miles")
                / (F.col("trip_duration_seconds") / F.lit(3600.0)),
            ).cast("double"),
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
            "hvfhs_license_num",
            "dispatching_base_num",
            "request_datetime",
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_location_id",
            "dropoff_location_id",
            "trip_miles",
            "base_passenger_fare",
        ],
    )
    rules = common_rules(df, context, quality, quality.fhv_max_duration_hours)
    rules.extend(
        [
            Rule(
                "MISSING_HVFHS_LICENSE",
                "ERROR",
                F.col("hvfhs_license_num").isNull()
                | (F.length("hvfhs_license_num") == 0),
            ),
            Rule(
                "UNKNOWN_HVFHS_LICENSE",
                "WARNING",
                F.col("hvfhs_license_num").isNotNull()
                & F.col("hvfhs_company_name").isNull(),
            ),
            Rule(
                "MISSING_DISPATCHING_BASE",
                "ERROR",
                F.col("dispatching_base_num").isNull()
                | (F.length("dispatching_base_num") == 0),
            ),
            Rule(
                "REQUEST_AFTER_PICKUP",
                "ERROR",
                F.col("request_datetime").isNull()
                | (F.col("request_datetime") > F.col("pickup_datetime")),
            ),
            Rule(
                "ON_SCENE_AFTER_PICKUP",
                "WARNING",
                F.col("on_scene_datetime").isNotNull()
                & (F.col("on_scene_datetime") > F.col("pickup_datetime")),
            ),
            Rule(
                "NEGATIVE_REQUEST_TO_PICKUP",
                "ERROR",
                F.col("request_to_pickup_seconds") < 0,
            ),
            Rule(
                "NEGATIVE_DRIVER_WAIT",
                "WARNING",
                F.col("driver_wait_seconds") < 0,
            ),
            Rule(
                "INVALID_TRIP_MILES",
                "ERROR",
                F.col("trip_miles").isNull()
                | (F.col("trip_miles") < 0)
                | (F.col("trip_miles") > quality.max_trip_distance_miles),
            ),
            Rule(
                "INVALID_TRIP_TIME",
                "ERROR",
                F.col("trip_time").isNull() | (F.col("trip_time") <= 0),
            ),
            Rule(
                "TRIP_TIME_MISMATCH",
                "WARNING",
                F.abs(F.col("trip_time") - F.col("trip_duration_seconds")) > 600,
            ),
            Rule(
                "SHARED_MATCH_WITHOUT_REQUEST",
                "WARNING",
                (F.col("shared_matched") == True)
                & (F.col("shared_requested") != True),
            ),
            Rule(
                "WAV_MATCH_WITHOUT_REQUEST",
                "WARNING",
                (F.col("wav_matched") == True) & (F.col("wav_requested") != True),
            ),
        ]
    )
    for name in flag_aliases:
        rules.append(
            Rule(
                f"INVALID_{name.upper()}",
                "WARNING",
                F.col(f"{name}_raw").isNotNull()
                & ~F.col(f"{name}_raw").isin(*quality.allowed_boolean_flags),
            )
        )
    negative = [
        F.when(F.col(name) < 0, F.lit(1)).otherwise(F.lit(0)) for name in money
    ]
    rules.append(
        Rule("NEGATIVE_FINANCIAL_AMOUNT", "ERROR", F.greatest(*negative) == 1)
    )
    return apply_rules(df, rules).drop(
        *[f"{name}_raw" for name in flag_aliases]
    )
