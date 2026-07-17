from __future__ import annotations

from typing import Any

from tlc_data_platform.core.settings import SilverSparkConfig


class SilverSparkProvider:
    """Creates one reusable SparkSession for reference and trip processing."""

    def __init__(self, config: SilverSparkConfig) -> None:
        self._config = config
        self._session: Any | None = None

    def get(self) -> Any:
        if self._session is None:
            from pyspark.sql import SparkSession

            self._session = (
                SparkSession.builder.appName(self._config.app_name)
                .master(self._config.master)
                .config("spark.driver.memory", self._config.driver_memory)
                .config("spark.sql.shuffle.partitions", str(self._config.shuffle_partitions))
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                .config("spark.sql.parquet.mergeSchema", "false")
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.sql.session.timeZone", "America/New_York")
                .config("spark.ui.enabled", "false")
                .getOrCreate()
            )
            self._session.sparkContext.setLogLevel(self._config.log_level)
        return self._session

    def close(self) -> None:
        if self._session is not None:
            self._session.stop()
            self._session = None
