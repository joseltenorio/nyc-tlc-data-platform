from __future__ import annotations

from typing import Any

from tlc_data_platform.core.settings import SilverQualityConfig
from tlc_data_platform.silver.models import SilverTransformContext
from tlc_data_platform.silver.transformers.common import (
    add_common_metadata,
    add_trip_business_id,
    apply_rules,
    col_from_aliases,
    normalize_string,
    taxi_rules,
)


TAXI_MONEY_COLUMNS = (
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
)


def transform_taxi(
    raw: Any,
    context: SilverTransformContext,
    quality: SilverQualityConfig,
    *,
    pickup_alias: str,
    dropoff_alias: str,
    vendor_values: tuple[int, ...],
    include_trip_type: bool,
    include_ehail_fee: bool,
) -> Any:
    from pyspark.sql import functions as F

    passenger_raw = col_from_aliases(raw, ["passenger_count"], "double")
    vendor_raw = col_from_aliases(raw, ["VendorID", "vendorid"], "long")
    rate_raw = col_from_aliases(raw, ["RatecodeID", "ratecodeid"], "long")
    payment_raw = col_from_aliases(raw, ["payment_type"], "long")
    store_raw = normalize_string(col_from_aliases(raw, ["store_and_fwd_flag"], "string"))

    df = raw.select(
        vendor_raw.alias("vendor_id_raw"),
        col_from_aliases(raw, [pickup_alias], "timestamp_ntz").alias("pickup_datetime"),
        col_from_aliases(raw, [dropoff_alias], "timestamp_ntz").alias("dropoff_datetime"),
        passenger_raw.alias("passenger_count_raw"),
        col_from_aliases(raw, ["trip_distance"], "double").alias("trip_distance"),
        rate_raw.alias("rate_code_id_raw"),
        store_raw.alias("store_and_fwd_flag_raw"),
        col_from_aliases(raw, ["PULocationID", "pulocationid"], "int").alias("pickup_location_id"),
        col_from_aliases(raw, ["DOLocationID", "dolocationid"], "int").alias("dropoff_location_id"),
        payment_raw.alias("payment_type_raw"),
        *[
            col_from_aliases(
                raw,
                ["Airport_fee", "airport_fee"] if name == "airport_fee" else [name],
                "double",
                0.0 if name not in {"fare_amount", "total_amount"} else None,
            ).alias(name)
            for name in TAXI_MONEY_COLUMNS
        ],
        col_from_aliases(raw, ["trip_type"], "long").alias("trip_type") if include_trip_type else F.lit(None).cast("long").alias("trip_type"),
        col_from_aliases(raw, ["ehail_fee"], "double", 0.0).alias("ehail_fee") if include_ehail_fee else F.lit(None).cast("double").alias("ehail_fee"),
    )

    df = (
        df.withColumn("vendor_normalized", ~F.col("vendor_id_raw").isin(*vendor_values) | F.col("vendor_id_raw").isNull())
        .withColumn("vendor_id", F.when(F.col("vendor_id_raw").isin(*vendor_values), F.col("vendor_id_raw")).otherwise(F.lit(99)).cast("int"))
        .withColumn("rate_code_normalized", ~F.col("rate_code_id_raw").isin(1, 2, 3, 4, 5, 6, 99) | F.col("rate_code_id_raw").isNull())
        .withColumn("rate_code_id", F.when(F.col("rate_code_id_raw").isin(1, 2, 3, 4, 5, 6, 99), F.col("rate_code_id_raw")).otherwise(F.lit(99)).cast("int"))
        .withColumn("payment_type_normalized", ~F.col("payment_type_raw").between(0, 6) | F.col("payment_type_raw").isNull())
        .withColumn("payment_type", F.when(F.col("payment_type_raw").between(0, 6), F.col("payment_type_raw")).otherwise(F.lit(5)).cast("int"))
        .withColumn("store_flag_normalized", ~F.col("store_and_fwd_flag_raw").isin(*quality.allowed_store_and_forward_flags) | F.col("store_and_fwd_flag_raw").isNull())
        .withColumn("store_and_fwd_flag", F.when(F.col("store_and_fwd_flag_raw").isin(*quality.allowed_store_and_forward_flags), F.col("store_and_fwd_flag_raw")).otherwise(F.lit("N")))
        .withColumn("passenger_count_imputed", F.col("passenger_count_raw").isNull() | (F.col("passenger_count_raw") == 0))
        .withColumn(
            "passenger_count",
            F.when(
                F.lit(quality.impute_zero_or_null_passenger_count)
                & (F.col("passenger_count_raw").isNull() | (F.col("passenger_count_raw") == 0)),
                F.lit(1),
            ).otherwise(F.round(F.col("passenger_count_raw"))).cast("int"),
        )
        .withColumn("trip_duration_seconds", F.timestamp_diff("SECOND", F.col("pickup_datetime"), F.col("dropoff_datetime")).cast("long"))
        .withColumn(
            "average_speed_mph",
            F.when(
                (F.col("trip_duration_seconds") > 0) & (F.col("trip_distance") >= 0),
                F.col("trip_distance") / (F.col("trip_duration_seconds") / F.lit(3600.0)),
            ).cast("double"),
        )
        .withColumn("airport_trip_flag", F.col("pickup_location_id").isin(1, 132, 138) | F.col("dropoff_location_id").isin(1, 132, 138))
        .withColumn(
            "fare_per_mile",
            F.when(F.col("trip_distance") > 0, F.col("fare_amount") / F.col("trip_distance")).cast("double"),
        )
        .withColumn(
            "tip_percentage",
            F.when(F.col("fare_amount") > 0, (F.col("tip_amount") / F.col("fare_amount")) * 100.0).cast("double"),
        )
        .withColumn(
            "revenue_per_minute",
            F.when(F.col("trip_duration_seconds") > 0, F.col("total_amount") / (F.col("trip_duration_seconds") / 60.0)).cast("double"),
        )
    )
    for name in TAXI_MONEY_COLUMNS:
        if name not in {"fare_amount", "total_amount"}:
            df = df.withColumn(name, F.coalesce(F.col(name), F.lit(0.0)).cast("double"))

    if include_trip_type:
        invalid_trip_type = ~F.col("trip_type").isin(1, 2) | F.col("trip_type").isNull()
        df = df.withColumn("trip_type_normalized", invalid_trip_type).withColumn(
            "trip_type", F.when(F.col("trip_type").isin(1, 2), F.col("trip_type")).otherwise(F.lit(None).cast("long"))
        )
    else:
        df = df.withColumn("trip_type_normalized", F.lit(False))

    df = add_common_metadata(df, context)
    df = add_trip_business_id(
        df,
        [
            "service_type",
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_location_id",
            "dropoff_location_id",
            "vendor_id",
            "trip_distance",
            "total_amount",
        ],
    )
    rules = taxi_rules(df, context, quality)
    if include_trip_type:
        from tlc_data_platform.silver.transformers.common import Rule

        rules.append(Rule("INVALID_TRIP_TYPE", "WARNING", F.col("trip_type_normalized")))
    df = apply_rules(df, rules)
    return df.drop(
        "vendor_id_raw",
        "rate_code_id_raw",
        "payment_type_raw",
        "store_and_fwd_flag_raw",
        "passenger_count_raw",
        "vendor_normalized",
        "rate_code_normalized",
        "payment_type_normalized",
        "store_flag_normalized",
        "trip_type_normalized",
    )
