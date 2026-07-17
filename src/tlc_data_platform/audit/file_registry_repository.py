from __future__ import annotations

from datetime import timedelta
from typing import Any

from tlc_data_platform.bronze.models import FileCandidate, FileOutcome, utc_now


class FileRegistryRepository:
    def __init__(
        self,
        collection: Any,
        claim_ttl_minutes: int,
        execution_repository: Any | None = None,
    ) -> None:
        self._collection = collection
        self._claim_ttl_minutes = claim_ttl_minutes
        self._execution_repository = execution_repository

    @staticmethod
    def _key(candidate: FileCandidate) -> dict[str, Any]:
        return {
            "service": candidate.service,
            "year": candidate.year,
            "month": candidate.month,
        }

    def get(self, candidate: FileCandidate) -> dict[str, Any] | None:
        return self._collection.find_one(self._key(candidate), {"_id": 0})

    def claim(self, candidate: FileCandidate, execution_id: str) -> bool:
        from pymongo.errors import DuplicateKeyError

        now = utc_now()
        expires_at = now + timedelta(minutes=self._claim_ttl_minutes)
        key = self._key(candidate)
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
                "status": "PENDING",
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
            return False
        if result.matched_count or result.upserted_id:
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
        result = self._collection.update_one(recovery_filter, update, upsert=False)
        return bool(result.matched_count)

    def _is_recoverable_claim(
        self,
        claim: dict[str, Any],
        execution_id: str,
        now: Any,
    ) -> bool:
        owner_execution_id = claim.get("execution_id")
        if not owner_execution_id or owner_execution_id == execution_id:
            return True
        expires_at = claim.get("expires_at")
        if expires_at is not None and expires_at < now:
            return True
        if self._execution_repository is None:
            return False
        execution = self._execution_repository.get(owner_execution_id)
        if execution is None:
            return True
        if execution.get("finished_at") is not None:
            return True
        return execution.get("status") in {"SUCCESS", "PARTIAL_SUCCESS", "FAILED"}

    @staticmethod
    def _recovery_filter(
        key: dict[str, Any],
        claim: dict[str, Any],
    ) -> dict[str, Any]:
        filter_doc = dict(key)
        if claim.get("execution_id") is not None:
            filter_doc["claim.execution_id"] = claim["execution_id"]
        if claim.get("claimed_at") is not None:
            filter_doc["claim.claimed_at"] = claim["claimed_at"]
        if claim.get("expires_at") is not None:
            filter_doc["claim.expires_at"] = claim["expires_at"]
        if len(filter_doc) == len(key):
            filter_doc["claim"] = {"$exists": True}
        return filter_doc

    def set_status(
        self,
        candidate: FileCandidate,
        execution_id: str,
        status: str,
        **fields: Any,
    ) -> None:
        self._collection.update_one(
            self._key(candidate),
            {
                "$set": {
                    "status": status,
                    "last_execution_id": execution_id,
                    "updated_at": utc_now(),
                    **fields,
                }
            },
            upsert=True,
        )

    def mark_ready(self, outcome: FileOutcome, execution_id: str) -> None:
        current = {
            "status": "READY",
            "sha256": outcome.sha256,
            "bytes_downloaded": outcome.bytes_downloaded,
            "local_path": outcome.local_path,
            "remote_metadata": (
                outcome.remote_metadata.to_dict() if outcome.remote_metadata else None
            ),
            "validation": outcome.validation.to_dict() if outcome.validation else None,
            "ready_at": outcome.finished_at,
            "execution_id": execution_id,
        }
        self._collection.update_one(
            self._key(outcome.candidate),
            {
                "$set": {
                    **self._key(outcome.candidate),
                    "status": "READY",
                    "sha256": outcome.sha256,
                    "bytes_downloaded": outcome.bytes_downloaded,
                    "local_path": outcome.local_path,
                    "remote_metadata": (
                        outcome.remote_metadata.to_dict()
                        if outcome.remote_metadata
                        else None
                    ),
                    "current": current,
                    "last_execution_id": execution_id,
                    "updated_at": utc_now(),
                },
                "$unset": {"claim": ""},
                "$setOnInsert": {"created_at": utc_now()},
            },
            upsert=True,
        )

    def mark_failed(self, outcome: FileOutcome, execution_id: str) -> None:
        self._collection.update_one(
            self._key(outcome.candidate),
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

    def mark_deferred(self, outcome: FileOutcome, execution_id: str) -> None:
        self._collection.update_one(
            self._key(outcome.candidate),
            {
                "$set": {
                    "status": outcome.status,
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

    def release_claim(self, candidate: FileCandidate, execution_id: str) -> None:
        self._collection.update_one(
            {**self._key(candidate), "claim.execution_id": execution_id},
            {"$unset": {"claim": ""}, "$set": {"updated_at": utc_now()}},
        )

    def release_claims_for_execution(self, execution_id: str) -> int:
        result = self._collection.update_many(
            {"claim.execution_id": execution_id},
            {"$unset": {"claim": ""}, "$set": {"updated_at": utc_now()}},
        )
        return int(result.modified_count)