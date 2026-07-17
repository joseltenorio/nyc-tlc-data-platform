from __future__ import annotations

from typing import Any

from tlc_data_platform.core.settings import SilverMongoCollections


class SilverIndexManager:
    def __init__(self, database: Any, names: SilverMongoCollections) -> None:
        self._db = database
        self._names = names

    def ensure_indexes(self) -> None:
        self._db[self._names.file_registry].create_index(
            [("service", 1), ("year", 1), ("month", 1)],
            unique=True,
            name="uq_silver_file_registry_period",
        )
        self._db[self._names.pipeline_executions].create_index(
            [("execution_id", 1)],
            unique=True,
            name="uq_silver_execution_id",
        )
        self._db[self._names.pipeline_executions].create_index(
            [("started_at", -1)], name="ix_silver_execution_started_at"
        )
        self._db[self._names.pipeline_executions].create_index(
            [("status", 1)], name="ix_silver_execution_status"
        )
        self._db[self._names.quality_results].create_index(
            [
                ("execution_id", 1),
                ("service", 1),
                ("year", 1),
                ("month", 1),
                ("rule_code", 1),
            ],
            unique=True,
            name="uq_silver_quality_execution_period_rule",
        )
        self._db[self._names.reconciliations].create_index(
            [
                ("execution_id", 1),
                ("service", 1),
                ("year", 1),
                ("month", 1),
            ],
            unique=True,
            name="uq_silver_reconciliation_execution_period",
        )
