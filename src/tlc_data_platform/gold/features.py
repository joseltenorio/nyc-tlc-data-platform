from __future__ import annotations

from typing import Any


# Gold owns feature engineering that is deterministic and reusable. Model-only
# operations (algorithm fitting, threshold selection and metrics) remain in ML.


def _add_exact_lag(frame: Any, hours: int, output_name: str) -> Any:
    """Joins the value observed at the exact same zone/service N hours before.

    A normal row-based `lag()` would incorrectly use the previous available row
    when an inactive zone has gaps. This timestamp join preserves the meaning of
    "24 hours ago" and leaves the value null when no observation exists.
    """
    from pyspark.sql import functions as F

    keys = ["pickup_location_id", "service_type"]
    previous = frame.select(
        *keys,
        F.expr(f"event_timestamp + INTERVAL {hours} HOURS").alias("event_timestamp"),
        F.col("trip_count").alias(output_name),
    )
    return frame.join(previous, [*keys, "event_timestamp"], "left")


def build_zone_hourly_demand_features(trip_activity: Any, dim_zone: Any) -> Any:
    """Builds the revised forecast grain: zone + hour + service.

    The former daily/service dataset was too coarse for a mobility case. This
    table supports future maps and rankings of expected demand by TLC zone.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    zones = dim_zone.select(
        F.col("zone_key").alias("pickup_location_id"),
        F.col("zone_name").alias("pickup_zone_name"),
        F.col("borough").alias("pickup_borough"),
        F.col("is_airport").alias("airport_zone_flag"),
    )
    demand = (
        trip_activity.groupBy(
            F.date_trunc("hour", "pickup_datetime").alias("event_timestamp"),
            F.col("pickup_zone_key").alias("pickup_location_id"),
            "service_type",
        )
        .agg(F.sum("trip_count").cast("double").alias("trip_count"))
        .join(zones, "pickup_location_id", "left")
    )

    for hours, name in (
        (1, "lag_1_hour"),
        (2, "lag_2_hours"),
        (24, "lag_24_hours"),
        (48, "lag_48_hours"),
        (168, "lag_168_hours"),
    ):
        demand = _add_exact_lag(demand, hours, name)

    order_seconds = F.col("event_timestamp").cast("long")
    partition = ["pickup_location_id", "service_type"]
    for hours, name in (
        (6, "rolling_mean_6h"),
        (24, "rolling_mean_24h"),
        (168, "rolling_mean_168h"),
    ):
        window = (
            Window.partitionBy(*partition)
            .orderBy(order_seconds)
            .rangeBetween(-hours * 3600, -1)
        )
        demand = demand.withColumn(name, F.avg("trip_count").over(window))

    std_window = (
        Window.partitionBy(*partition)
        .orderBy(order_seconds)
        .rangeBetween(-24 * 3600, -1)
    )
    return (
        demand.withColumn("rolling_stddev_24h", F.stddev_pop("trip_count").over(std_window))
        .withColumn("same_hour_previous_day", F.col("lag_24_hours"))
        .withColumn("same_hour_previous_week", F.col("lag_168_hours"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_hour", F.hour("event_timestamp"))
        .withColumn("hour_of_day", F.hour("event_timestamp"))
        .withColumn("day_of_week", F.dayofweek("event_timestamp"))
        .withColumn("month", F.month("event_timestamp"))
        .withColumn("is_weekend", F.dayofweek("event_timestamp").isin(1, 7))
        .withColumn("is_peak_hour", F.hour("event_timestamp").isin(7, 8, 9, 16, 17, 18, 19))
        .select(
            "event_date",
            "event_hour",
            "event_timestamp",
            "pickup_location_id",
            "pickup_zone_name",
            "pickup_borough",
            "service_type",
            "trip_count",
            "lag_1_hour",
            "lag_2_hours",
            "lag_24_hours",
            "lag_48_hours",
            "lag_168_hours",
            "rolling_mean_6h",
            "rolling_mean_24h",
            "rolling_mean_168h",
            "rolling_stddev_24h",
            "same_hour_previous_day",
            "same_hour_previous_week",
            "hour_of_day",
            "day_of_week",
            "month",
            "is_weekend",
            "is_peak_hour",
            "airport_zone_flag",
        )
    )


def build_zone_profile_features(
    trip_activity: Any,
    hvfhv_operations: Any,
    dim_zone: Any,
) -> Any:
    """Creates one behavioral profile per TLC pickup zone for K-Means."""
    from pyspark.sql import functions as F

    pickup = trip_activity.groupBy(F.col("pickup_zone_key").alias("zone_key")).agg(
        F.sum("trip_count").alias("total_pickups"),
        F.avg("trip_duration_seconds").alias("average_duration_seconds"),
        F.avg("trip_distance_miles").alias("average_distance_miles"),
        F.avg("average_speed_mph").alias("average_speed_mph"),
        F.avg(F.col("night_trip_flag").cast("double")).alias("night_trip_share"),
        F.avg(F.col("weekend_trip_flag").cast("double")).alias("weekend_trip_share"),
        F.avg(F.col("airport_trip_flag").cast("double")).alias("airport_trip_share"),
        F.sum(F.when(F.col("service_type") == "yellow", F.col("trip_count")).otherwise(0)).alias("yellow_trips"),
        F.sum(F.when(F.col("service_type") == "green", F.col("trip_count")).otherwise(0)).alias("green_trips"),
        F.sum(F.when(F.col("service_type") == "fhv", F.col("trip_count")).otherwise(0)).alias("fhv_trips"),
        F.sum(F.when(F.col("service_type") == "fhvhv", F.col("trip_count")).otherwise(0)).alias("fhvhv_trips"),
        F.countDistinct("service_type").alias("service_diversity"),
    )
    dropoff = trip_activity.groupBy(F.col("dropoff_zone_key").alias("zone_key")).agg(
        F.sum("trip_count").alias("total_dropoffs")
    )
    hourly = trip_activity.groupBy(
        F.col("pickup_zone_key").alias("zone_key"),
        F.date_trunc("hour", "pickup_datetime").alias("event_hour"),
    ).agg(F.sum("trip_count").alias("hourly_trip_count"))
    variability = hourly.groupBy("zone_key").agg(
        F.count("event_hour").alias("active_hours"),
        F.stddev_pop("hourly_trip_count").alias("demand_stddev"),
        F.avg("hourly_trip_count").alias("average_hourly_demand"),
    ).withColumn(
        "demand_coefficient_of_variation",
        F.when(
            F.col("average_hourly_demand") > 0,
            F.col("demand_stddev") / F.col("average_hourly_demand"),
        ),
    )
    wait = hvfhv_operations.groupBy(F.col("pickup_zone_key").alias("zone_key")).agg(
        F.avg("request_to_pickup_seconds").alias("average_request_to_pickup_seconds"),
        F.avg(F.col("shared_matched").cast("double")).alias("shared_match_share"),
        F.avg(F.col("wav_matched").cast("double")).alias("wav_match_share"),
    )
    zones = dim_zone.select(
        "zone_key", "location_id", "zone_name", "borough", "service_zone", "is_airport"
    )

    result = (
        zones.join(pickup, "zone_key", "left")
        .join(dropoff, "zone_key", "left")
        .join(variability, "zone_key", "left")
        .join(wait, "zone_key", "left")
        .fillna(
            0,
            subset=[
                "total_pickups",
                "total_dropoffs",
                "yellow_trips",
                "green_trips",
                "fhv_trips",
                "fhvhv_trips",
                "service_diversity",
                "active_hours",
            ],
        )
    )
    for service in ("yellow", "green", "fhv", "fhvhv"):
        result = result.withColumn(
            f"{service}_share",
            F.when(F.col("total_pickups") > 0, F.col(f"{service}_trips") / F.col("total_pickups")).otherwise(0.0),
        )
    return result.drop("yellow_trips", "green_trips", "fhv_trips", "fhvhv_trips")


def build_hvfhv_wait_features(hvfhv_operations: Any, dim_zone: Any) -> Any:
    """Selects only information available when an HVFHV trip is requested.

    Post-pickup outcomes such as trip duration, driver wait, fare and driver pay
    are deliberately excluded from the predictor table to prevent target leakage.
    The observed request-to-pickup time is retained only as the training target.
    """
    from pyspark.sql import functions as F

    zones = dim_zone.select(
        F.col("zone_key").alias("pickup_zone_key"),
        F.col("zone_name").alias("pickup_zone_name"),
        F.col("borough").alias("pickup_borough"),
        F.col("is_airport").alias("airport_zone_flag"),
    )
    return (
        hvfhv_operations.join(zones, "pickup_zone_key", "left")
        .filter(
            F.col("request_datetime").isNotNull()
            & F.col("request_to_pickup_seconds").isNotNull()
            & (F.col("request_to_pickup_seconds") >= 0)
        )
        .select(
            "trip_key",
            "request_datetime",
            F.hour("request_datetime").alias("request_hour"),
            F.dayofweek("request_datetime").alias("request_day_of_week"),
            F.month("request_datetime").alias("request_month"),
            F.dayofweek("request_datetime").isin(1, 7).alias("is_weekend"),
            F.hour("request_datetime").isin(7, 8, 9, 16, 17, 18, 19).alias("is_peak_hour"),
            "pickup_zone_key",
            "pickup_zone_name",
            "pickup_borough",
            "airport_zone_flag",
            "provider_key",
            "hvfhs_license_num",
            "hvfhs_company_name",
            F.coalesce("shared_requested", F.lit(False)).alias("shared_requested"),
            F.coalesce("access_a_ride", F.lit(False)).alias("access_a_ride"),
            F.coalesce("wav_requested", F.lit(False)).alias("wav_requested"),
            "request_to_pickup_seconds",
            "source_year",
            "source_month",
        )
    )
