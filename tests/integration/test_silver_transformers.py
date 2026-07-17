from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from tlc_data_platform.silver.enrichment import SilverReferenceData, enrich_trip
from tlc_data_platform.silver.master import to_master
from tlc_data_platform.silver.models import SilverTransformContext
from tlc_data_platform.silver.transformers.fhv import transform as transform_fhv
from tlc_data_platform.silver.transformers.fhvhv import transform as transform_fhvhv
from tlc_data_platform.silver.transformers.green import transform as transform_green
from tlc_data_platform.silver.transformers.yellow import transform as transform_yellow


def context(service: str) -> SilverTransformContext:
    return SilverTransformContext(
        service,
        2025,
        1,
        f"{service}_tripdata_2025-01.parquet",
        "sha",
        "bronze",
        "silver",
    )


def test_enrichment_and_master_contract(spark, app_config):
    raw = spark.createDataFrame(
        [
            {
                "dispatching_base_num": "B01234",
                "pickup_datetime": "2025-01-06 10:00:00",
                "dropOff_datetime": "2025-01-06 10:30:00",
                "PUlocationID": 132,
                "DOlocationID": 138,
                "SR_Flag": "1",
                "Affiliated_base_number": "B01234",
            }
        ]
    )
    transformed = transform_fhv(raw, context("fhv"), app_config.silver.quality)
    zones = spark.createDataFrame(
        [
            (132, "Queens", "JFK Airport", "Airports", True, "JFK"),
            (138, "Queens", "LaGuardia Airport", "Airports", True, "LGA"),
        ],
        [
            "location_id",
            "borough",
            "zone_name",
            "service_zone",
            "is_airport",
            "airport_name",
        ],
    )
    bases = spark.createDataFrame(
        [("B01234", "TEST BASE", "TEST DBA", "Black Car", "Active")],
        [
            "base_license_number",
            "base_name",
            "doing_business_as",
            "base_type",
            "status",
        ],
    )
    enriched = enrich_trip(
        transformed,
        "fhv",
        SilverReferenceData(taxi_zones=zones, base_lookup=bases),
    )
    row = enriched.first()
    assert row.pickup_zone_name == "JFK Airport"
    assert row.dispatching_base_name == "TEST BASE"
    master = to_master(enriched, "fhv").first()
    assert master.service_id == 3
    assert master.trip_count == 1
    assert master.shared_matched is True
    assert master.pickup_borough == "Queens"


def test_yellow_transform_splits_valid_and_invalid_rows(spark, app_config):
    rows = [
        (
            1,
            "2025-01-02 10:00:00",
            "2025-01-02 10:20:00",
            1.0,
            3.0,
            1,
            "N",
            132,
            138,
            1,
            20.0,
            0.0,
            0.5,
            3.0,
            0.0,
            1.0,
            24.5,
            2.5,
            1.75,
            0.75,
        ),
        (
            1,
            "2025-01-02 11:00:00",
            "2025-01-02 10:20:00",
            1.0,
            3.0,
            1,
            "N",
            132,
            138,
            1,
            20.0,
            0.0,
            0.5,
            3.0,
            0.0,
            1.0,
            24.5,
            2.5,
            1.75,
            0.75,
        ),
    ]
    columns = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "Airport_fee",
        "cbd_congestion_fee",
    ]
    raw = spark.createDataFrame(rows, columns)
    result = transform_yellow(raw, context("yellow"), app_config.silver.quality)
    assert result.filter("quality_status != 'REJECTED'").count() == 1
    assert (
        result.filter(
            "array_contains(quality_error_codes, 'INVALID_DATE_ORDER')"
        ).count()
        == 1
    )
    valid = result.filter("quality_status != 'REJECTED'").first()
    assert valid.pickup_hour == 10
    assert valid.fare_per_mile > 0
    assert valid.tip_percentage == pytest.approx(15.0)


def test_green_normalizes_passenger_count_and_trip_type(spark, app_config):
    row = {
        "VendorID": 2,
        "lpep_pickup_datetime": "2025-01-03 23:00:00",
        "lpep_dropoff_datetime": "2025-01-03 23:15:00",
        "store_and_fwd_flag": "n",
        "RatecodeID": 1,
        "PULocationID": 41,
        "DOLocationID": 42,
        "passenger_count": 0.0,
        "trip_distance": 2.0,
        "fare_amount": 12.0,
        "extra": 1.0,
        "mta_tax": 0.5,
        "tip_amount": 2.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 16.5,
        "payment_type": 1,
        "trip_type": 2,
        "congestion_surcharge": 0.0,
        "ehail_fee": 0.0,
        "cbd_congestion_fee": 0.75,
    }
    result = transform_green(
        spark.createDataFrame([row]),
        context("green"),
        app_config.silver.quality,
    ).first()
    assert result.passenger_count == 1
    assert result.trip_type == 2
    assert result.store_and_fwd_flag == "N"
    assert result.night_trip_flag is True
    assert "PASSENGER_COUNT_IMPUTED" in result.quality_warning_codes


def test_fhv_invalid_shared_flag_is_a_warning(spark, app_config):
    row = {
        "dispatching_base_num": " b01234 ",
        "pickup_datetime": "2025-01-04 10:00:00",
        "dropOff_datetime": "2025-01-04 10:30:00",
        "PUlocationID": 132,
        "DOlocationID": 138,
        "SR_Flag": "X",
        "Affiliated_base_number": "b05678",
    }
    result = transform_fhv(
        spark.createDataFrame([row]),
        context("fhv"),
        app_config.silver.quality,
    ).first()
    assert result.dispatching_base_num == "B01234"
    assert result.shared_ride_flag is None
    assert "INVALID_SHARED_RIDE_FLAG" in result.quality_warning_codes


def test_hvfhv_normalizes_flags_and_company(spark, app_config):
    row = {
        "hvfhs_license_num": "HV0003",
        "dispatching_base_num": "B02877",
        "originating_base_num": "B02877",
        "request_datetime": "2025-01-05 09:55:00",
        "on_scene_datetime": "2025-01-05 09:58:00",
        "pickup_datetime": "2025-01-05 10:00:00",
        "dropoff_datetime": "2025-01-05 10:20:00",
        "PULocationID": 132,
        "DOLocationID": 138,
        "trip_miles": 10.0,
        "trip_time": 1200,
        "base_passenger_fare": 30.0,
        "tolls": 0.0,
        "bcf": 0.75,
        "sales_tax": 2.66,
        "congestion_surcharge": 2.75,
        "airport_fee": 2.5,
        "tips": 5.0,
        "driver_pay": 22.0,
        "shared_request_flag": "y",
        "shared_match_flag": "Y",
        "access_a_ride_flag": "N",
        "wav_request_flag": "N",
        "wav_match_flag": "N",
        "cbd_congestion_fee": 0.75,
    }
    result = transform_fhvhv(
        spark.createDataFrame([row]),
        context("fhvhv"),
        app_config.silver.quality,
    ).first()
    assert result.hvfhs_company_name == "Uber"
    assert result.shared_requested is True
    assert result.shared_matched is True
    assert result.request_to_pickup_seconds == 300
    assert result.driver_wait_seconds == 120
    assert result.quality_status in {"VALID", "WARNING"}


