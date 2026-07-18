from __future__ import annotations

from typing import Any


class MLModelRepository:
    """Registers model versions and keeps one active version per model family."""

    def __init__(self, collection: Any) -> None:
        self._collection = collection

    def register(self, result: Any) -> None:
        self._collection.update_many(
            {"model_name": result.model_name, "status": "ACTIVE"},
            {"$set": {"status": "SUPERSEDED"}},
        )
        self._collection.update_one(
            {"model_id": result.model_id},
            {"$set": {**result.to_dict(), "status": "ACTIVE"}},
            upsert=True,
        )
