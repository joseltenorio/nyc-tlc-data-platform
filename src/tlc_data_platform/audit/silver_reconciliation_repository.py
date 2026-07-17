from __future__ import annotations

from typing import Any

from tlc_data_platform.silver.models import SilverFileOutcome, utc_now


class SilverReconciliationRepository:
    def __init__(self, collection: Any) -> None:
        self._collection = collection

    def insert(self, outcome: SilverFileOutcome, execution_id: str) -> None:
        self._collection.insert_one(
            {
                "execution_id": execution_id,
                "service": outcome.source.service,
                "year": outcome.source.year,
                "month": outcome.source.month,
                "bronze_metadata_rows": outcome.source.bronze_num_rows,
                "silver_rows_read": outcome.rows_read,
                "silver_rows_valid": outcome.rows_valid,
                "silver_rows_rejected": outcome.rows_rejected,
                "balance": outcome.rows_read - outcome.rows_valid - outcome.rows_rejected,
                "status": outcome.reconciliation_status,
                "recorded_at": utc_now(),
            }
        )
