from __future__ import annotations

from datetime import datetime
from typing import Any


class MLRunRepository:
    """Stores one training run and its final aggregate result."""

    def __init__(self, collection: Any) -> None:
        self._collection = collection

    def start(self, run_id: str, started_at: datetime, models: list[str]) -> None:
        self._collection.insert_one(
            {
                "run_id": run_id,
                "layer": "ml",
                "status": "RUNNING",
                "started_at": started_at,
                "requested_models": models,
            }
        )

    def finish(self, summary: Any) -> None:
        self._collection.update_one(
            {"run_id": summary.run_id}, {"$set": summary.to_dict()}, upsert=True
        )

    def fail(self, run_id: str, finished_at: datetime, error: Exception) -> None:
        self._collection.update_one(
            {"run_id": run_id},
            {
                "$set": {
                    "status": "FAILED",
                    "finished_at": finished_at,
                    "error_type": type(error).__name__,
                    "error_message": str(error)[:2000],
                }
            },
            upsert=True,
        )
