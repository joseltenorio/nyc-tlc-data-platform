from __future__ import annotations

from typing import Any


# Los marts son tablas deliberadamente pequeñas. Streamlit podrá consultarlas
# con DuckDB sin lanzar Spark ni leer cada viaje individual en cada interacción.


def build_marts(
    trip_activity: Any,
    taxi_financial: Any,
    hvfhv_operations: Any,
    dim_zone: Any,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    pickup_zones = dim_zone.select(
        F.col("zone_key").alias("pickup_zone_key"),
        F.col("zone_name").alias("pickup_zone_name"),
        F.col("borough").alias("pickup_borough"),
    )
    dropoff_zones = dim_zone.select(
        F.col("zone_key").alias("dropoff_zone_key"),
        F.col("zone_name").alias("dropoff_zone_name"),
        F.col("borough").alias("dropoff_borough"),
    )
    trips = trip_activity.join(pickup_zones, "pickup_zone_key", "left").join(
        dropoff_zones, "dropoff_zone_key", "left"
    )

    monthly_keys = ["source_year", "source_month", "service_type"]
    executive_monthly = trips.groupBy(*monthly_keys).agg(
        F.sum("trip_count").alias("trip_count"),
        F.avg("trip_duration_seconds").alias("average_duration_seconds"),
        F.avg("trip_distance_miles").alias("average_distance_miles"),
        F.avg("average_speed_mph").alias("average_speed_mph"),
        F.sum(F.col("airport_trip_flag").cast("long")).alias("airport_trip_count"),
        F.sum(F.col("quality_warning_count")).alias("quality_warning_count"),
    )

    service_totals = trips.groupBy("source_year", "source_month").agg(
        F.sum("trip_count").alias("all_services_trip_count")
    )
    service_share = executive_monthly.join(
        service_totals, ["source_year", "source_month"], "left"
    ).withColumn(
        "service_share",
        F.when(
            F.col("all_services_trip_count") > 0,
            F.col("trip_count") / F.col("all_services_trip_count"),
        ),
    )

    daily_demand = trips.groupBy(
        F.to_date("pickup_datetime").alias("event_date"), "service_type"
    ).agg(F.sum("trip_count").alias("trip_count"))

    zone_demand = trips.groupBy(
        "source_year",
        "source_month",
        "service_type",
        "pickup_zone_key",
        "pickup_zone_name",
        "pickup_borough",
    ).agg(
        F.sum("trip_count").alias("pickup_trip_count"),
        F.avg("trip_duration_seconds").alias("average_duration_seconds"),
        F.avg("trip_distance_miles").alias("average_distance_miles"),
        F.avg("average_speed_mph").alias("average_speed_mph"),
    )

    od_routes = trips.groupBy(
        "source_year",
        "source_month",
        "service_type",
        "pickup_zone_key",
        "pickup_zone_name",
        "pickup_borough",
        "dropoff_zone_key",
        "dropoff_zone_name",
        "dropoff_borough",
    ).agg(
        F.sum("trip_count").alias("trip_count"),
        F.avg("trip_duration_seconds").alias("average_duration_seconds"),
        F.avg("trip_distance_miles").alias("average_distance_miles"),
        F.avg("average_speed_mph").alias("average_speed_mph"),
    )

    time_heatmap = trips.groupBy(
        "service_type",
        F.dayofweek("pickup_datetime").alias("day_of_week"),
        F.hour("pickup_datetime").alias("hour_of_day"),
    ).agg(
        F.sum("trip_count").alias("trip_count"),
        F.avg("trip_duration_seconds").alias("average_duration_seconds"),
    )

    financial_profile = taxi_financial.groupBy(*monthly_keys).agg(
        F.sum("trip_count").alias("trip_count"),
        F.sum("fare_amount").alias("fare_amount"),
        F.sum("tip_amount").alias("tip_amount"),
        F.sum("tolls_amount").alias("tolls_amount"),
        F.sum("total_amount").alias("total_amount"),
        F.avg("total_amount").alias("average_total_amount"),
        F.avg("tip_percentage").alias("average_tip_percentage"),
    )

    airport_activity = trips.groupBy(
        "source_year", "source_month", "service_type", "airport_trip_flag"
    ).agg(
        F.sum("trip_count").alias("trip_count"),
        F.avg("trip_duration_seconds").alias("average_duration_seconds"),
        F.avg("trip_distance_miles").alias("average_distance_miles"),
    )

    taxi_with_zone = taxi_financial.join(pickup_zones, "pickup_zone_key", "left")
    profitability_drivers = taxi_with_zone.groupBy(
        "service_type",
        "source_year",
        "source_month",
        "pickup_zone_key",
        "pickup_zone_name",
        "pickup_borough",
        "time_key",
        "airport_trip_flag",
    ).agg(
        F.sum("trip_count").alias("trip_count"),
        F.avg("total_amount").alias("average_total_amount"),
        F.avg("fare_per_mile").alias("average_fare_per_mile"),
        F.avg("revenue_per_minute").alias("average_revenue_per_minute"),
        F.avg("tip_percentage").alias("average_tip_percentage"),
    )

    # Tip analysis is intentionally restricted to card payments. TLC does not
    # record cash tips, so mixing cash records would create a false zero-tip rate.
    tip_behavior = taxi_with_zone.filter(F.col("payment_type_key") == 1).groupBy(
        "service_type",
        "source_year",
        "source_month",
        "pickup_zone_key",
        "pickup_zone_name",
        "pickup_borough",
        "time_key",
    ).agg(
        F.sum("trip_count").alias("credit_card_trip_count"),
        F.avg("tip_amount").alias("average_tip_amount"),
        F.avg("tip_percentage").alias("average_tip_percentage"),
        F.expr("percentile_approx(tip_percentage, 0.5)").alias("median_tip_percentage"),
    )

    operational_efficiency = trips.groupBy(
        "service_type", "source_year", "source_month", F.hour("pickup_datetime").alias("hour_of_day")
    ).agg(
        F.sum("trip_count").alias("trip_count"),
        F.avg("trip_duration_seconds").alias("average_duration_seconds"),
        F.expr("percentile_approx(trip_duration_seconds, 0.95)").alias("p95_duration_seconds"),
        F.avg("average_speed_mph").alias("average_speed_mph"),
    )

    route_efficiency = od_routes.select(
        "source_year",
        "source_month",
        "service_type",
        "pickup_zone_key",
        "pickup_zone_name",
        "dropoff_zone_key",
        "dropoff_zone_name",
        "trip_count",
        "average_duration_seconds",
        "average_distance_miles",
        "average_speed_mph",
    )

    zone_congestion = trips.groupBy(
        "source_year",
        "source_month",
        "service_type",
        "pickup_zone_key",
        "pickup_zone_name",
        "pickup_borough",
        F.hour("pickup_datetime").alias("hour_of_day"),
    ).agg(
        F.sum("trip_count").alias("trip_count"),
        F.avg("average_speed_mph").alias("average_speed_mph"),
        F.avg("trip_duration_seconds").alias("average_duration_seconds"),
        F.expr("percentile_approx(trip_duration_seconds, 0.95)").alias("p95_duration_seconds"),
    ).withColumn(
        "congestion_index",
        F.when(
            F.col("average_speed_mph") > 0,
            F.col("p95_duration_seconds") / F.col("average_speed_mph"),
        ),
    )

    zone_month_total = trips.groupBy(
        "source_year", "source_month", "pickup_zone_key"
    ).agg(F.sum("trip_count").alias("zone_total_trip_count"))
    service_competition = zone_demand.join(
        zone_month_total, ["source_year", "source_month", "pickup_zone_key"], "left"
    ).withColumn(
        "zone_service_share",
        F.when(
            F.col("zone_total_trip_count") > 0,
            F.col("pickup_trip_count") / F.col("zone_total_trip_count"),
        ),
    )

    hv_zone = hvfhv_operations.join(pickup_zones, "pickup_zone_key", "left")
    hvfhv_service_quality = hv_zone.groupBy(
        "source_year",
        "source_month",
        "provider_key",
        "hvfhs_company_name",
        "pickup_zone_key",
        "pickup_zone_name",
        "pickup_borough",
        "request_time_key",
    ).agg(
        F.sum("trip_count").alias("trip_count"),
        F.avg("request_to_pickup_seconds").alias("average_request_to_pickup_seconds"),
        F.expr("percentile_approx(request_to_pickup_seconds, 0.95)").alias("p95_request_to_pickup_seconds"),
        F.avg("driver_wait_seconds").alias("average_driver_wait_seconds"),
        F.avg("driver_pay").alias("average_driver_pay"),
    )

    shared_ride_performance = hvfhv_operations.groupBy(
        "source_year", "source_month", "provider_key", "hvfhs_company_name"
    ).agg(
        F.sum("trip_count").alias("trip_count"),
        F.sum(F.col("shared_requested").cast("long")).alias("shared_requested_count"),
        F.sum(F.col("shared_matched").cast("long")).alias("shared_matched_count"),
    ).withColumn(
        "shared_match_rate",
        F.when(
            F.col("shared_requested_count") > 0,
            F.col("shared_matched_count") / F.col("shared_requested_count"),
        ),
    )

    accessibility_performance = hvfhv_operations.groupBy(
        "source_year", "source_month", "provider_key", "hvfhs_company_name"
    ).agg(
        F.sum("trip_count").alias("trip_count"),
        F.sum(F.col("access_a_ride").cast("long")).alias("access_a_ride_count"),
        F.sum(F.col("wav_requested").cast("long")).alias("wav_requested_count"),
        F.sum(F.col("wav_matched").cast("long")).alias("wav_matched_count"),
    ).withColumn(
        "wav_match_rate",
        F.when(
            F.col("wav_requested_count") > 0,
            F.col("wav_matched_count") / F.col("wav_requested_count"),
        ),
    )

    return {
        "executive_monthly": executive_monthly,
        "service_share": service_share,
        "daily_demand": daily_demand,
        "zone_demand": zone_demand,
        "od_routes": od_routes,
        "time_heatmap": time_heatmap,
        "financial_profile": financial_profile,
        "airport_activity": airport_activity,
        "profitability_drivers": profitability_drivers,
        "tip_behavior": tip_behavior,
        "operational_efficiency": operational_efficiency,
        "route_efficiency": route_efficiency,
        "zone_congestion": zone_congestion,
        "service_competition": service_competition,
        "hvfhv_service_quality": hvfhv_service_quality,
        "shared_ride_performance": shared_ride_performance,
        "accessibility_performance": accessibility_performance,
    }
