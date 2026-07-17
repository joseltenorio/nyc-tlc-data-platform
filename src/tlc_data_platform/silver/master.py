from __future__ import annotations

from typing import Any

SERVICE_IDS = {"yellow": 1, "green": 2, "fhv": 3, "fhvhv": 4}


def _col_or_null(df: Any, name: str, dtype: str) -> Any:
    from pyspark.sql import functions as F

    expression = (
        F.col(name).cast(dtype)
        if name in df.columns
        else F.lit(None).cast(dtype)
    )
    return expression.alias(name)


def to_master(df: Any, service: str) -> Any:
    """Projects valid service records into the conforming Silver trip contract."""
    from pyspark.sql import functions as F

    vendor = {
        "yellow": "vendor_id",
        "green": "vendor_id",
        "fhv": "dispatching_base_num",
        "fhvhv": "hvfhs_license_num",
    }[service]
    distance = "trip_miles" if service == "fhvhv" else "trip_distance"
    fare = "base_passenger_fare" if service == "fhvhv" else "fare_amount"
    tolls = "tolls" if service == "fhvhv" else "tolls_amount"
    tips = "tips" if service == "fhvhv" else "tip_amount"
    shared_matched = F.coalesce(
        _col_or_null(df, "shared_matched", "boolean"),
        _col_or_null(df, "shared_ride_flag", "boolean"),
    )

    return df.select(
        F.lit(SERVICE_IDS[service]).cast("int").alias("service_id"),
        F.lit(service).alias("service_type"),
        F.lit(1).cast("long").alias("trip_count"),
        F.col("trip_business_id"),
        _col_or_null(df, vendor, "string").alias("vendor_id_or_license"),
        _col_or_null(df, "hvfhs_company_name", "string"),
        _col_or_null(df, "dispatching_base_num", "string"),
        _col_or_null(df, "dispatching_base_name", "string"),
        _col_or_null(df, "dispatching_base_dba", "string"),
        _col_or_null(df, "dispatching_base_type", "string"),
        _col_or_null(df, "originating_base_num", "string"),
        _col_or_null(df, "originating_base_name", "string"),
        _col_or_null(df, "affiliated_base_number", "string").alias(
            "affiliated_base_num"
        ),
        _col_or_null(df, "affiliated_base_name", "string"),
        _col_or_null(df, "request_datetime", "timestamp_ntz"),
        _col_or_null(df, "on_scene_datetime", "timestamp_ntz"),
        F.col("pickup_datetime").cast("timestamp_ntz"),
        F.col("dropoff_datetime").cast("timestamp_ntz"),
        F.col("pickup_date").cast("date"),
        F.col("dropoff_date").cast("date"),
        F.col("pickup_hour").cast("int"),
        F.col("pickup_day_of_week").cast("int"),
        F.col("weekend_trip_flag").cast("boolean"),
        F.col("night_trip_flag").cast("boolean"),
        F.col("pickup_location_id").cast("int"),
        _col_or_null(df, "pickup_zone_name", "string"),
        _col_or_null(df, "pickup_borough", "string"),
        _col_or_null(df, "pickup_service_zone", "string"),
        F.col("dropoff_location_id").cast("int"),
        _col_or_null(df, "dropoff_zone_name", "string"),
        _col_or_null(df, "dropoff_borough", "string"),
        _col_or_null(df, "dropoff_service_zone", "string"),
        _col_or_null(df, "passenger_count", "int"),
        _col_or_null(df, distance, "double").alias("trip_distance"),
        F.col("trip_duration_seconds").cast("long"),
        _col_or_null(df, "trip_time", "long").alias(
            "reported_trip_time_seconds"
        ),
        _col_or_null(df, "request_to_pickup_seconds", "long"),
        _col_or_null(df, "driver_wait_seconds", "long"),
        _col_or_null(df, "average_speed_mph", "double"),
        _col_or_null(df, "rate_code_id", "int"),
        _col_or_null(df, "payment_type", "int"),
        _col_or_null(df, "trip_type", "int"),
        _col_or_null(df, fare, "double").alias("fare_amount"),
        _col_or_null(df, "extra", "double"),
        _col_or_null(df, "mta_tax", "double"),
        _col_or_null(df, tips, "double").alias("tip_amount"),
        _col_or_null(df, tolls, "double").alias("tolls_amount"),
        _col_or_null(df, "improvement_surcharge", "double"),
        _col_or_null(df, "total_amount", "double"),
        _col_or_null(df, "congestion_surcharge", "double"),
        _col_or_null(df, "airport_fee", "double"),
        _col_or_null(df, "cbd_congestion_fee", "double"),
        _col_or_null(df, "black_car_fund", "double"),
        _col_or_null(df, "sales_tax", "double"),
        _col_or_null(df, "driver_pay", "double"),
        _col_or_null(df, "fare_per_mile", "double"),
        _col_or_null(df, "tip_percentage", "double"),
        _col_or_null(df, "revenue_per_minute", "double"),
        _col_or_null(df, "store_and_fwd_flag", "string"),
        _col_or_null(df, "shared_requested", "boolean"),
        shared_matched.alias("shared_matched"),
        _col_or_null(df, "access_a_ride", "boolean"),
        _col_or_null(df, "wav_requested", "boolean"),
        _col_or_null(df, "wav_matched", "boolean"),
        _col_or_null(df, "airport_trip_flag", "boolean"),
        F.col("quality_status"),
        F.col("quality_warning_codes"),
        F.col("quality_warning_count"),
        F.col("source_file"),
        F.col("source_year"),
        F.col("source_month"),
        F.col("bronze_sha256"),
        F.col("bronze_execution_id"),
        F.col("silver_execution_id"),
        F.col("silver_processed_at"),
    )
