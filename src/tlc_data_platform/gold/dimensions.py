from __future__ import annotations

from datetime import date
from typing import Any


# Estas dimensiones son "conformadas": las mismas claves se reutilizan en
# todas las tablas de hechos. Esto evita que cada dashboard interprete fechas,
# servicios o zonas de una forma distinta.


def build_date_dimension(spark: Any, start_year: int, end_year: int) -> Any:
    """Creates one row per calendar day for the configured analytical range."""
    from pyspark.sql import functions as F

    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    number_of_days = (end - start).days + 1
    return (
        spark.range(number_of_days)
        .select(F.date_add(F.lit(start.isoformat()), F.col("id").cast("int")).alias("full_date"))
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month_number", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
        .withColumn("week_number", F.weekofyear("full_date"))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("day_of_week_number", F.dayofweek("full_date"))
        .withColumn("day_of_week_name", F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
        .withColumn("is_month_start", F.col("full_date") == F.trunc("full_date", "month"))
        .withColumn("is_month_end", F.col("full_date") == F.last_day("full_date"))
        .select(
            "date_key",
            "full_date",
            "year",
            "quarter",
            "month_number",
            "month_name",
            "year_month",
            "week_number",
            "day_of_month",
            "day_of_week_number",
            "day_of_week_name",
            "is_weekend",
            "is_month_start",
            "is_month_end",
        )
    )


def build_time_dimension(spark: Any) -> Any:
    """Creates the 24 hourly buckets used by the facts and dashboards."""
    from pyspark.sql import functions as F

    return (
        spark.range(24)
        .select(F.col("id").cast("int").alias("hour"))
        .withColumn("time_key", F.col("hour"))
        .withColumn("hour_label", F.format_string("%02d:00", F.col("hour")))
        .withColumn(
            "time_band",
            F.when(F.col("hour") <= 5, "MADRUGADA")
            .when(F.col("hour") <= 11, "MANANA")
            .when(F.col("hour") <= 16, "TARDE")
            .when(F.col("hour") <= 21, "PICO_NOCTURNO")
            .otherwise("NOCHE"),
        )
        .withColumn("is_peak_hour", F.col("hour").isin(7, 8, 9, 16, 17, 18, 19))
        .withColumn("is_night", (F.col("hour") <= 5) | (F.col("hour") >= 22))
        .select("time_key", "hour", "hour_label", "time_band", "is_peak_hour", "is_night")
    )


def build_service_dimension(spark: Any) -> Any:
    """Documents which analytical capabilities are valid for each TLC service."""
    rows = [
        (1, "yellow", "Yellow Taxi", "TAXI", True, True, False, False),
        (2, "green", "Green Taxi", "TAXI", True, True, False, False),
        (3, "fhv", "For-Hire Vehicle", "FHV", False, False, True, False),
        (4, "fhvhv", "High Volume FHV", "HVFHV", True, False, True, True),
    ]
    return spark.createDataFrame(
        rows,
        "service_key int, service_code string, service_name string, service_category string, "
        "supports_financial_analysis boolean, supports_passenger_analysis boolean, "
        "supports_shared_ride_analysis boolean, supports_accessibility_analysis boolean",
    )


def build_zone_dimension(taxi_zones: Any) -> Any:
    """Uses TLC LocationID as a stable natural/surrogate key for zone analytics."""
    from pyspark.sql import functions as F

    return (
        taxi_zones.select(
            F.col("location_id").cast("int").alias("zone_key"),
            F.col("location_id").cast("int").alias("location_id"),
            F.col("zone_name").cast("string"),
            F.col("borough").cast("string"),
            F.col("service_zone").cast("string"),
        )
        .withColumn("is_airport", F.col("location_id").isin(1, 132, 138))
        .withColumn(
            "airport_name",
            F.when(F.col("location_id") == 132, "JFK")
            .when(F.col("location_id") == 138, "LGA")
            .when(F.col("location_id") == 1, "EWR")
            .otherwise(F.lit(None).cast("string")),
        )
        .dropDuplicates(["location_id"])
    )


def build_provider_dimension(master: Any) -> Any:
    """Conforms taxi vendors and FHV/HVFHV bases into one provider dimension."""
    from pyspark.sql import functions as F

    provider_code = F.coalesce(
        F.col("vendor_id_or_license"),
        F.col("dispatching_base_num"),
        F.col("originating_base_num"),
    )
    provider_name = F.coalesce(
        F.col("hvfhs_company_name"),
        F.col("dispatching_base_name"),
        F.col("dispatching_base_dba"),
        provider_code,
    )
    return (
        master.select(
            F.col("service_type"),
            provider_code.cast("string").alias("provider_code"),
            provider_name.cast("string").alias("provider_name"),
            F.col("dispatching_base_num").cast("string"),
            F.col("originating_base_num").cast("string"),
            F.col("affiliated_base_num").cast("string"),
        )
        .filter(F.col("provider_code").isNotNull())
        .dropDuplicates(["service_type", "provider_code"])
        .withColumn(
            "provider_key",
            F.abs(F.xxhash64("service_type", "provider_code")).cast("long"),
        )
        .withColumn(
            "provider_type",
            F.when(F.col("service_type").isin("yellow", "green"), "TAXI_VENDOR")
            .when(F.col("service_type") == "fhvhv", "HVFHS")
            .otherwise("FHV_BASE"),
        )
        .select(
            "provider_key",
            "provider_code",
            "provider_name",
            "provider_type",
            "service_type",
            "dispatching_base_num",
            "originating_base_num",
            "affiliated_base_num",
        )
    )


def build_payment_type_dimension(spark: Any) -> Any:
    rows = [
        (0, "Flex Fare", False),
        (1, "Credit card", True),
        (2, "Cash", False),
        (3, "No charge", False),
        (4, "Dispute", False),
        (5, "Unknown", False),
        (6, "Voided trip", False),
        (-1, "Not applicable", False),
    ]
    return spark.createDataFrame(
        rows,
        "payment_type_key int, payment_type_name string, is_electronic boolean",
    )


def build_rate_code_dimension(spark: Any) -> Any:
    rows = [
        (1, "Standard rate", False, False),
        (2, "JFK", True, False),
        (3, "Newark", True, False),
        (4, "Nassau or Westchester", False, False),
        (5, "Negotiated fare", False, True),
        (6, "Group ride", False, False),
        (99, "Null/unknown", False, False),
        (-1, "Not applicable", False, False),
    ]
    return spark.createDataFrame(
        rows,
        "rate_code_key int, rate_code_name string, is_airport_rate boolean, is_negotiated_rate boolean",
    )


def build_trip_type_dimension(spark: Any) -> Any:
    rows = [(1, "Street-hail"), (2, "Dispatch"), (-1, "Not applicable")]
    return spark.createDataFrame(rows, "trip_type_key int, trip_type_name string")
