from __future__ import annotations

from typing import Any

from tlc_data_platform.bronze.models import FileOutcome, utc_now


class FileVersionRepository:
    def __init__(self, collection: Any) -> None:
        self._collection = collection

    def insert_current(self, outcome: FileOutcome, execution_id: str) -> None:
        if not outcome.sha256:
            raise ValueError("No se puede registrar una versión sin sha256")
        document = {
            "service": outcome.candidate.service,
            "year": outcome.candidate.year,
            "month": outcome.candidate.month,
            "sha256": outcome.sha256,
            "file_name": outcome.candidate.file_name,
            "source_url": outcome.candidate.url,
            "bytes_downloaded": outcome.bytes_downloaded,
            "local_path": outcome.local_path,
            "archived_path": None,
            "remote_metadata": (
                outcome.remote_metadata.to_dict() if outcome.remote_metadata else None
            ),
            "validation": outcome.validation.to_dict() if outcome.validation else None,
            "execution_id": execution_id,
            "downloaded_at": outcome.finished_at,
            "created_at": utc_now(),
        }
        self._collection.update_one(
            {
                "service": outcome.candidate.service,
                "year": outcome.candidate.year,
                "month": outcome.candidate.month,
                "sha256": outcome.sha256,
            },
            {"$setOnInsert": document},
            upsert=True,
        )

    def mark_archived(
        self,
        service: str,
        year: int,
        month: int,
        sha256: str | None,
        archived_path: str,
        execution_id: str,
    ) -> None:
        if not sha256:
            return
        self._collection.update_one(
            {
                "service": service,
                "year": year,
                "month": month,
                "sha256": sha256,
            },
            {
                "$set": {
                    "archived_path": archived_path,
                    "archived_at": utc_now(),
                    "archived_by_execution_id": execution_id,
                }
            },
        )