from dataclasses import replace
from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from tlc_data_platform.core.settings import resolve_silver_selection
from tlc_data_platform.silver.audit import SilverAuditRepositories
from tlc_data_platform.silver.models import SilverPeriodState, SilverSourceFile
from tlc_data_platform.silver.pipeline import SilverPipeline


class StaticSparkProvider:
    def __init__(self, spark):
        self.spark = spark

    def get(self):
        return self.spark

    def close(self):
        pass


class Mongo:
    def database(self):
        return {}

    def close(self):
        pass


class Executions:
    def __init__(self):
        self.started = None
        self.finished = None

    def start(self, *args):
        self.started = args

    def finish(self, summary):
        self.finished = summary

    def fail(self, *args):
        raise AssertionError(f"Unexpected execution failure: {args}")


class Registry:
    def __init__(self):
        self.ready = None
        self.failed = None

    def is_unchanged(self, source, outputs_exist):
        return False

    def claim(self, source, execution_id):
        return True

    def mark_ready(self, outcome, execution_id):
        self.ready = outcome

    def mark_failed(self, outcome, execution_id):
        self.failed = outcome

    def release_claims_for_execution(self, execution_id):
        return 0


class Quality:
    def __init__(self):
        self.outcome = None

    def replace_for_outcome(self, outcome, execution_id):
        self.outcome = outcome


class Reconciliations:
    def __init__(self):
        self.outcome = None

    def insert(self, outcome, execution_id):
        self.outcome = outcome


class Catalog:
    def __init__(self, source):
        self.source = source

    def list(self, selection):
        return [self.source], [
            SilverPeriodState(
                self.source.service,
                self.source.year,
                self.source.month,
                "BRONZE_READY",
                source_path=str(self.source.path),
                source_sha256=self.source.source_sha256,
            )
        ]


def test_silver_pipeline_writes_curated_rejected_and_master(
    spark, app_config, tmp_path
):
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
    bronze_path = tmp_path / "yellow_tripdata_2025-01.parquet"
    from pyspark.sql import functions as F

    bronze = spark.createDataFrame(rows, columns)
    bronze = (
        bronze.withColumn(
            "tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp_ntz")
        ).withColumn(
            "tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp_ntz")
        )
    )
    bronze.write.mode("overwrite").parquet(str(bronze_path))
    source = SilverSourceFile(
        "yellow",
        2025,
        1,
        bronze_path,
        "sha",
        "bronze-run",
        2,
        "READY",
    )
    execution = replace(
        app_config.silver.execution,
        require_reference_data=False,
        refresh_references_if_missing=False,
        refresh_references_before_run=False,
    )
    config = replace(app_config, silver=replace(app_config.silver, execution=execution))
    registry = Registry()
    audit = SilverAuditRepositories(
        executions=Executions(),
        registry=registry,
        quality=Quality(),
        reconciliations=Reconciliations(),
    )
    pipeline = SilverPipeline(
        config,
        spark_provider=StaticSparkProvider(spark),
        mongo_provider=Mongo(),
        audit=audit,
        source_catalog=Catalog(source),
    )
    selection = resolve_silver_selection(
        config,
        "silver-run",
        services=["yellow"],
        start_year=2025,
        end_year=2025,
        months=[1],
    )
    summary = pipeline.run(selection, execution_type="run")
    assert summary.status == "SUCCESS"
    assert summary.rows_read == 2
    assert summary.rows_valid == 1
    assert summary.rows_rejected == 1
    assert registry.ready is not None and registry.failed is None
    assert spark.read.parquet(registry.ready.curated_path).count() == 1
    assert spark.read.parquet(registry.ready.rejected_path).count() == 1
    master = spark.read.parquet(registry.ready.master_path)
    assert master.count() == 1
    assert "pickup_zone_name" in master.columns
    assert "trip_count" in master.columns
