from __future__ import annotations

from typing import Any


class GoldDatasetRepository:
    """Maintains the current physical state of every Gold dataset."""

    def __init__(self, collection: Any) -> None:
        self._collection = collection

    def upsert(self, execution_id: str, result: Any) -> None:
        payload = result.to_dict()
        payload.update({"last_execution_id": execution_id, "layer": "gold"})
        self._collection.update_one(
            {"dataset_name": result.dataset_name},
            {"$set": payload},
            upsert=True,
        )
