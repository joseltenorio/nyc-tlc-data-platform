from __future__ import annotations

from typing import Any

from tlc_data_platform.core.settings import SparkConfig


class SparkProvider:
    """Lazily creates exactly one SparkSession for the validation phase."""

    def __init__(self, config: SparkConfig) -> None:
        self._config = config
        self._session: Any | None = None

    def get(self) -> Any:
        if self._session is None:
            from pyspark.sql import SparkSession

            builder = (
                SparkSession.builder.appName(self._config.app_name)
                .master(self._config.master)
                .config("spark.driver.memory", self._config.driver_memory)
                .config("spark.sql.parquet.mergeSchema", "false")
                .config("spark.ui.enabled", "false")
            )
            self._session = builder.getOrCreate()
            self._session.sparkContext.setLogLevel(self._config.log_level)
        return self._session

    def close(self) -> None:
        if self._session is not None:
            self._session.stop()
            self._session = None
