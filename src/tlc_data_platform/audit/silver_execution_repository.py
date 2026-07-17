from __future__ import annotations

from datetime import datetime
from typing import Any

from tlc_data_platform.silver.models import SilverExecutionSummary


class SilverExecutionRepository:
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
                "layer": "silver",
                "execution_type": execution_type,
                "status": "RUNNING",
                "started_at": started_at,
                "selection": selection,
            }
        )

    def finish(self, summary: SilverExecutionSummary) -> None:
        self._collection.update_one(
            {"execution_id": summary.execution_id},
            {"$set": summary.to_dict()},
            upsert=True,
        )

    def fail(
        self,
        execution_id: str,
        finished_at: datetime,
        error: Exception,
        manifest_path: str | None,
    ) -> None:
        self._collection.update_one(
            {"execution_id": execution_id},
            {
                "$set": {
                    "status": "FAILED",
                    "finished_at": finished_at,
                    "error_type": type(error).__name__,
                    "error_message": str(error),
                    "manifest_path": manifest_path,
                }
            },
            upsert=True,
        )

    def get(self, execution_id: str) -> dict[str, Any] | None:
        return self._collection.find_one({"execution_id": execution_id}, {"_id": 0})

    def is_active(self, execution_id: str) -> bool:
        execution = self.get(execution_id)
        return bool(
            execution
            and execution.get("status") == "RUNNING"
            and execution.get("finished_at") is None
        )
