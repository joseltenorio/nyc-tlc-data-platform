from __future__ import annotations

from datetime import timedelta
from typing import Any

from tlc_data_platform.silver.models import SilverFileOutcome, SilverSourceFile, utc_now


class SilverFileRegistryRepository:
    def __init__(
        self,
        collection: Any,
        claim_ttl_minutes: int = 360,
        execution_repository: Any | None = None,
    ) -> None:
        self._collection = collection
        self._claim_ttl_minutes = claim_ttl_minutes
        self._execution_repository = execution_repository

    @staticmethod
    def key(source: SilverSourceFile) -> dict[str, Any]:
        return {"service": source.service, "year": source.year, "month": source.month}

    def get(self, source: SilverSourceFile) -> dict[str, Any] | None:
        return self._collection.find_one(self.key(source), {"_id": 0})

    def claim(self, source: SilverSourceFile, execution_id: str) -> bool:
        from pymongo.errors import DuplicateKeyError

        now = utc_now()
        expires_at = now + timedelta(minutes=self._claim_ttl_minutes)
        key = self.key(source)
        filter_doc = {
            **key,
            "$or": [
                {"claim": {"$exists": False}},
                {"claim.expires_at": {"$lt": now}},
                {"claim.execution_id": execution_id},
            ],
        }
        update = {
            "$set": {
                **key,
                "status": "PROCESSING",
                "claim": {
                    "execution_id": execution_id,
                    "claimed_at": now,
                    "expires_at": expires_at,
                },
                "updated_at": now,
            },
            "$setOnInsert": {"created_at": now},
        }
        try:
            result = self._collection.update_one(filter_doc, update, upsert=True)
        except DuplicateKeyError:
            result = None
        if result is not None and (result.matched_count or result.upserted_id):
            return True

        current = self._collection.find_one(key, {"_id": 0, "claim": 1})
        if current is None:
            try:
                retry = self._collection.update_one(filter_doc, update, upsert=True)
            except DuplicateKeyError:
                return False
            return bool(retry.matched_count or retry.upserted_id)

        claim = current.get("claim") or {}
        if not self._is_recoverable_claim(claim, execution_id, now):
            return False
        recovery_filter = self._recovery_filter(key, claim)
        recovered = self._collection.update_one(recovery_filter, update, upsert=False)
        return bool(recovered.matched_count)

    def _is_recoverable_claim(
        self,
        claim: dict[str, Any],
        execution_id: str,
        now: Any,
    ) -> bool:
        owner = claim.get("execution_id")
        if not owner or owner == execution_id:
            return True
        expires_at = claim.get("expires_at")
        if expires_at is not None and expires_at < now:
            return True
        if self._execution_repository is None:
            return False
        execution = self._execution_repository.get(owner)
        if execution is None:
            return True
        if execution.get("finished_at") is not None:
            return True
        return execution.get("status") in {
            "SUCCESS",
            "PARTIAL_SUCCESS",
            "FAILED",
            "NO_INPUT",
        }

    @staticmethod
    def _recovery_filter(
        key: dict[str, Any], claim: dict[str, Any]
    ) -> dict[str, Any]:
        filter_doc = dict(key)
        for field in ("execution_id", "claimed_at", "expires_at"):
            if claim.get(field) is not None:
                filter_doc[f"claim.{field}"] = claim[field]
        if len(filter_doc) == len(key):
            filter_doc["claim"] = {"$exists": True}
        return filter_doc

    def is_unchanged(self, source: SilverSourceFile, outputs_exist: bool) -> bool:
        current = self.get(source)
        return bool(
            current
            and current.get("status") == "READY"
            and current.get("source_sha256")
            and current.get("source_sha256") == source.source_sha256
            and outputs_exist
        )

    def mark_ready(self, outcome: SilverFileOutcome, execution_id: str) -> None:
        self._collection.update_one(
            self.key(outcome.source),
            {
                "$set": {
                    **self.key(outcome.source),
                    "status": "READY",
                    "source_sha256": outcome.source.source_sha256,
                    "bronze_execution_id": outcome.source.bronze_execution_id,
                    "curated_path": outcome.curated_path,
                    "rejected_path": outcome.rejected_path,
                    "master_path": outcome.master_path,
                    "rows_read": outcome.rows_read,
                    "rows_valid": outcome.rows_valid,
                    "rows_rejected": outcome.rows_rejected,
                    "warning_rows": outcome.warning_rows,
                    "rule_counts": outcome.rule_counts,
                    "rule_severities": outcome.rule_severities,
                    "reconciliation_status": outcome.reconciliation_status,
                    "last_execution_id": execution_id,
                    "ready_at": outcome.finished_at,
                    "updated_at": utc_now(),
                },
                "$unset": {"claim": "", "last_error": ""},
                "$setOnInsert": {"created_at": utc_now()},
            },
            upsert=True,
        )

    def mark_failed(self, outcome: SilverFileOutcome, execution_id: str) -> None:
        self._collection.update_one(
            self.key(outcome.source),
            {
                "$set": {
                    "status": "FAILED",
                    "last_execution_id": execution_id,
                    "last_error": {
                        "type": outcome.error_type,
                        "message": outcome.error_message,
                        "at": outcome.finished_at,
                    },
                    "updated_at": utc_now(),
                },
                "$unset": {"claim": ""},
            },
            upsert=True,
        )

    def release_claim(self, source: SilverSourceFile, execution_id: str) -> None:
        self._collection.update_one(
            {**self.key(source), "claim.execution_id": execution_id},
            {"$unset": {"claim": ""}, "$set": {"updated_at": utc_now()}},
        )

    def release_claims_for_execution(self, execution_id: str) -> int:
        result = self._collection.update_many(
            {"claim.execution_id": execution_id},
            {"$unset": {"claim": ""}, "$set": {"updated_at": utc_now()}},
        )
        return int(result.modified_count)
