from __future__ import annotations

from typing import Any

from tlc_data_platform.core.settings import MongoConfig


class MongoIndexManager:
    def __init__(self, database: Any, config: MongoConfig) -> None:
        self._db = database
        self._config = config

    def ensure_indexes(self) -> None:
        names = self._config.collections
        self._db[names.file_registry].create_index(
            [("service", 1), ("year", 1), ("month", 1)],
            unique=True,
            name="uq_file_registry_period",
        )
        self._db[names.file_versions].create_index(
            [("service", 1), ("year", 1), ("month", 1), ("sha256", 1)],
            unique=True,
            name="uq_file_versions_period_sha",
        )
        self._db[names.file_availability].create_index(
            [("execution_id", 1), ("service", 1), ("year", 1), ("month", 1)],
            name="ix_availability_execution_period",
        )
        self._db[names.pipeline_executions].create_index(
            [("started_at", -1)], name="ix_execution_started_at"
        )
        self._db[names.pipeline_executions].create_index(
            [("status", 1)], name="ix_execution_status"
        )