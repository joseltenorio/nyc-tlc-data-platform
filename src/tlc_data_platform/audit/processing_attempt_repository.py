from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4


class ProcessingAttemptRepository:
    """Records individual stages so the audit dashboard can count retries/attempts."""

    def __init__(self, collection: Any) -> None:
        self._collection = collection

    def start(self, execution_id: str, layer: str, stage: str, subject: str, started_at: datetime) -> str:
        attempt_id = str(uuid4())
        attempt_number = self._collection.count_documents(
            {"execution_id": execution_id, "layer": layer, "stage": stage, "subject": subject}
        ) + 1
        self._collection.insert_one(
            {
                "attempt_id": attempt_id,
                "execution_id": execution_id,
                "layer": layer,
                "stage": stage,
                "subject": subject,
                "attempt_number": attempt_number,
                "status": "RUNNING",
                "started_at": started_at,
            }
        )
        return attempt_id

    def finish(
        self,
        attempt_id: str,
        finished_at: datetime,
        *,
        status: str,
        rows_read: int | None = None,
        rows_written: int | None = None,
        error: Exception | None = None,
    ) -> None:
        document = self._collection.find_one(
            {"attempt_id": attempt_id}, {"started_at": 1}
        ) or {}
        started_at = document.get("started_at")
        duration_seconds = (
            (finished_at - started_at).total_seconds()
            if isinstance(started_at, datetime)
            else None
        )
        update: dict[str, Any] = {
            "status": status,
            "finished_at": finished_at,
            "duration_seconds": duration_seconds,
            "rows_read": rows_read,
            "rows_written": rows_written,
        }
        if error is not None:
            update["error_type"] = type(error).__name__
            update["error_message"] = str(error)[:2000]
        self._collection.update_one({"attempt_id": attempt_id}, {"$set": update})
