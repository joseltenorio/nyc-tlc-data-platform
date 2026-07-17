from __future__ import annotations

from typing import Any

from tlc_data_platform.silver.models import SilverFileOutcome, utc_now


class SilverQualityRepository:
    def __init__(self, collection: Any) -> None:
        self._collection = collection

    def replace_for_outcome(
        self, outcome: SilverFileOutcome, execution_id: str
    ) -> None:
        key = {
            "execution_id": execution_id,
            "service": outcome.source.service,
            "year": outcome.source.year,
            "month": outcome.source.month,
        }
        self._collection.delete_many(key)
        documents = [
            {
                **key,
                "rule_code": rule,
                "severity": outcome.rule_severities.get(rule, "UNKNOWN"),
                "affected_rows": count,
                "recorded_at": utc_now(),
            }
            for rule, count in sorted(outcome.rule_counts.items())
        ]
        if documents:
            self._collection.insert_many(documents)
