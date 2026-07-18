from __future__ import annotations

from datetime import datetime
from typing import Any


class GoldExecutionRepository:
    """Persists one document per Gold pipeline execution."""

    def __init__(self, collection: Any) -> None:
        self._collection = collection

    def start(
        self,
        execution_id: str,
        execution_type: str,
        started_at: datetime,
        selection: dict[str, Any],
    ) -> None:
        self._collection.insert_one(
            {
                "execution_id": execution_id,
                "layer": "gold",
                "execution_type": execution_type,
                "status": "RUNNING",
                "started_at": started_at,
                "selection": selection,
            }
        )

    def finish(self, summary: Any) -> None:
        self._collection.update_one(
            {"execution_id": summary.execution_id},
            {"$set": summary.to_dict()},
            upsert=True,
        )

    def fail(self, execution_id: str, finished_at: datetime, error: Exception) -> None:
        self._collection.update_one(
            {"execution_id": execution_id},
            {
                "$set": {
                    "layer": "gold",
                    "status": "FAILED",
                    "finished_at": finished_at,
                    "error_type": type(error).__name__,
                    "error_message": str(error)[:2000],
                }
            },
            upsert=True,
        )
