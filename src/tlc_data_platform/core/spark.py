from __future__ import annotations

from typing import Any
from uuid import uuid4

from tlc_data_platform.core.resource_guard import (
    SparkResourceGuard,
    cleanup_stale_spark_dirs,
    reset_spark_local_dir,
)
from tlc_data_platform.core.settings import SparkConfig


class SparkProvider:
    """Lazily creates one small SparkSession for Bronze Parquet validation."""

    def __init__(self, config: SparkConfig) -> None:
        self._config = config
        self._local_dir = config.local_dir / f"run-{uuid4().hex[:10]}"
        self._session: Any | None = None

    def get(self) -> Any:
        if self._session is None:
            from pyspark.sql import SparkSession

            cleanup_stale_spark_dirs(self._config.local_dir)
            reset_spark_local_dir(self._local_dir)
            self._session = (
                SparkSession.builder.appName(self._config.app_name)
                .master(self._config.master)
                .config("spark.driver.memory", self._config.driver_memory)
                .config("spark.driver.maxResultSize", "512m")
                .config("spark.local.dir", str(self._local_dir.resolve()))
                .config("spark.sql.files.maxPartitionBytes", str(64 * 1024**2))
                .config("spark.sql.parquet.mergeSchema", "false")
                .config("spark.ui.enabled", "false")
                .getOrCreate()
            )
            self._session.sparkContext.setLogLevel(self._config.log_level)
        return self._session

    def guard(self) -> SparkResourceGuard:
        return SparkResourceGuard(
            self.get(),
            self._local_dir,
            max_temp_bytes=self._config.max_temp_bytes,
            minimum_free_space_bytes=self._config.minimum_free_space_bytes,
        )

    def close(self) -> None:
        if self._session is not None:
            self._session.stop()
            self._session = None
        reset_spark_local_dir(self._local_dir)
