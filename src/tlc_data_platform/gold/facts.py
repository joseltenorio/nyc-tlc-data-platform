from __future__ import annotations

from typing import Any


SERVICE_KEYS = {"yellow": 1, "green": 2, "fhv": 3, "fhvhv": 4}


def _service_key() -> Any:
    from pyspark.sql import functions as F

    expression = F.lit(None).cast("int")
    for service, key in reversed(list(SERVICE_KEYS.items())):
        expression = F.when(F.col("service_type") == service, F.lit(key)).otherwise(expression)
    return expression


def _provider_key() -> Any:
    from pyspark.sql import functions as F

    provider_code = F.coalesce(
        F.col("vendor_id_or_license"),
        F.col("dispatching_base_num"),
        F.col("originating_base_num"),
    )
    return F.when(
        provider_code.isNotNull(),
        F.abs(F.xxhash64("service_type", provider_code)).cast("long"),
    )


def build_trip_activity_fact(master: Any) -> Any:
    """Builds the cross-service fact at one row per accepted Silver trip.

    Only attributes that have the same semantic meaning across services are
    included here. Service-specific finance and HVFHV operational fields live
    in separate facts, avoiding a giant sparse table full of misleading nulls.
    """
    from pyspark.sql import functions as F

    return master.select(
        F.col("trip_business_id").alias("trip_key"),
        F.date_format("pickup_date", "yyyyMMdd").cast("int").alias("pickup_date_key"),
        F.col("pickup_hour").cast("int").alias("pickup_time_key"),
        F.date_format("dropoff_date", "yyyyMMdd").cast("int").alias("dropoff_date_key"),
        F.hour("dropoff_datetime").cast("int").alias("dropoff_time_key"),
        _service_key().alias("service_key"),
        F.col("service_type"),
        F.col("pickup_location_id").cast("int").alias("pickup_zone_key"),
        F.col("dropoff_location_id").cast("int").alias("dropoff_zone_key"),
        _provider_key().alias("provider_key"),
        F.col("trip_count").cast("long"),
        F.col("pickup_datetime"),
        F.col("dropoff_datetime"),
        F.col("trip_duration_seconds").cast("long"),
        F.col("trip_distance").cast("double").alias("trip_distance_miles"),
        F.col("passenger_count").cast("int"),
        F.col("average_speed_mph").cast("double"),
        F.col("weekend_trip_flag").cast("boolean"),
        F.col("night_trip_flag").cast("boolean"),
        F.col("airport_trip_flag").cast("boolean"),
        F.col("quality_warning_count").cast("int"),
        F.col("quality_status"),
        F.col("source_year").cast("int"),
        F.col("source_month").cast("int"),
        F.col("silver_execution_id"),
        F.col("silver_processed_at"),
    )


def build_taxi_financial_fact(master: Any) -> Any:
    """Builds financial measures only for Yellow and Green taxi trips."""
    from pyspark.sql import functions as F

    taxi = master.filter(F.col("service_type").isin("yellow", "green"))
    return taxi.select(
        F.col("trip_business_id").alias("trip_key"),
        F.date_format("pickup_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("pickup_hour").cast("int").alias("time_key"),
        _service_key().alias("service_key"),
        F.col("service_type"),
        F.col("pickup_location_id").cast("int").alias("pickup_zone_key"),
        F.col("dropoff_location_id").cast("int").alias("dropoff_zone_key"),
        _provider_key().alias("provider_key"),
        F.coalesce(F.col("payment_type"), F.lit(-1)).cast("int").alias("payment_type_key"),
        F.coalesce(F.col("rate_code_id"), F.lit(-1)).cast("int").alias("rate_code_key"),
        F.coalesce(F.col("trip_type"), F.lit(-1)).cast("int").alias("trip_type_key"),
        F.col("fare_amount").cast("double"),
        F.col("extra").cast("double").alias("extra_amount"),
        F.col("mta_tax").cast("double").alias("mta_tax_amount"),
        F.col("tip_amount").cast("double"),
        F.col("tolls_amount").cast("double"),
        F.col("improvement_surcharge").cast("double"),
        F.col("congestion_surcharge").cast("double"),
        F.col("airport_fee").cast("double"),
        F.col("cbd_congestion_fee").cast("double"),
        F.col("total_amount").cast("double"),
        F.col("fare_per_mile").cast("double"),
        F.col("tip_percentage").cast("double"),
        F.col("revenue_per_minute").cast("double"),
        F.col("trip_distance").cast("double").alias("trip_distance_miles"),
        F.col("trip_duration_seconds").cast("long"),
        F.col("airport_trip_flag").cast("boolean"),
        F.col("trip_count").cast("long"),
        F.col("source_year").cast("int"),
        F.col("source_month").cast("int"),
    )


def build_hvfhv_operations_fact(master: Any) -> Any:
    """Builds the HVFHV fact used for service-quality and wait-risk analysis.

    `request_to_pickup_seconds` is the business target for the new classifier.
    Predictor construction later excludes post-pickup fields so the model does
    not learn from information that would be unavailable when a trip is requested.
    """
    from pyspark.sql import functions as F

    hv = master.filter(F.col("service_type") == "fhvhv")
    passenger_charge = (
        F.coalesce(F.col("fare_amount"), F.lit(0.0))
        + F.coalesce(F.col("tolls_amount"), F.lit(0.0))
        + F.coalesce(F.col("black_car_fund"), F.lit(0.0))
        + F.coalesce(F.col("sales_tax"), F.lit(0.0))
        + F.coalesce(F.col("congestion_surcharge"), F.lit(0.0))
        + F.coalesce(F.col("airport_fee"), F.lit(0.0))
        + F.coalesce(F.col("cbd_congestion_fee"), F.lit(0.0))
        + F.coalesce(F.col("tip_amount"), F.lit(0.0))
    )
    return hv.select(
        F.col("trip_business_id").alias("trip_key"),
        F.date_format(F.to_date("request_datetime"), "yyyyMMdd").cast("int").alias("request_date_key"),
        F.hour("request_datetime").cast("int").alias("request_time_key"),
        F.date_format("pickup_date", "yyyyMMdd").cast("int").alias("pickup_date_key"),
        F.col("pickup_hour").cast("int").alias("pickup_time_key"),
        _service_key().alias("service_key"),
        F.lit("fhvhv").alias("service_type"),
        F.col("pickup_location_id").cast("int").alias("pickup_zone_key"),
        F.col("dropoff_location_id").cast("int").alias("dropoff_zone_key"),
        _provider_key().alias("provider_key"),
        F.col("vendor_id_or_license").cast("string").alias("hvfhs_license_num"),
        F.col("hvfhs_company_name"),
        F.col("dispatching_base_num"),
        F.col("originating_base_num"),
        F.col("request_datetime"),
        F.col("on_scene_datetime"),
        F.col("pickup_datetime"),
        F.col("dropoff_datetime"),
        F.col("trip_distance").cast("double").alias("trip_miles"),
        F.col("reported_trip_time_seconds").cast("long").alias("trip_time_seconds"),
        F.col("trip_duration_seconds").cast("long"),
        F.col("request_to_pickup_seconds").cast("long"),
        F.col("driver_wait_seconds").cast("long"),
        F.col("fare_amount").cast("double").alias("base_passenger_fare"),
        F.col("tolls_amount").cast("double").alias("tolls"),
        F.col("black_car_fund").cast("double"),
        F.col("sales_tax").cast("double"),
        F.col("congestion_surcharge").cast("double"),
        F.col("airport_fee").cast("double"),
        F.col("cbd_congestion_fee").cast("double"),
        F.col("tip_amount").cast("double").alias("tips"),
        F.col("driver_pay").cast("double"),
        passenger_charge.cast("double").alias("passenger_charge_amount"),
        F.col("shared_requested").cast("boolean"),
        F.col("shared_matched").cast("boolean"),
        F.col("access_a_ride").cast("boolean"),
        F.col("wav_requested").cast("boolean"),
        F.col("wav_matched").cast("boolean"),
        F.col("airport_trip_flag").cast("boolean"),
        F.col("weekend_trip_flag").cast("boolean"),
        F.col("night_trip_flag").cast("boolean"),
        F.col("trip_count").cast("long"),
        F.col("source_year").cast("int"),
        F.col("source_month").cast("int"),
    )
