from __future__ import annotations

from typing import Any

from tlc_data_platform.bronze.models import AvailabilityRecord


class AvailabilityRepository:
    def __init__(self, collection: Any) -> None:
        self._collection = collection

    def insert_many(self, records: list[AvailabilityRecord]) -> None:
        if records:
            self._collection.insert_many([record.to_dict() for record in records])