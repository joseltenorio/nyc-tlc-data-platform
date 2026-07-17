from datetime import timedelta
from pathlib import Path

from tlc_data_platform.audit.silver_file_registry_repository import (
    SilverFileRegistryRepository,
)
from tlc_data_platform.silver.models import SilverFileOutcome, SilverSourceFile, utc_now


class Result:
    def __init__(self, matched=0, upserted_id=None, modified=0):
        self.matched_count = matched
        self.upserted_id = upserted_id
        self.modified_count = modified


class Executions:
    def __init__(self, docs=None):
        self.docs = docs or {}

    def get(self, execution_id):
        return self.docs.get(execution_id)


class Collection:
    def __init__(self, doc=None):
        self.doc = doc

    def find_one(self, key, projection=None):
        if self.doc is None:
            return None
        if any(
            self.doc.get(k) != v
            for k, v in key.items()
            if "." not in k and k != "$or"
        ):
            return None
        if projection == {"_id": 0, "claim": 1}:
            return {"claim": self.doc.get("claim")}
        return dict(self.doc)

    def update_one(self, key, update, upsert=False):
        if self._matches(key):
            if self.doc is None:
                self.doc = {}
            self.doc.update(update.get("$set", {}))
            for field in update.get("$unset", {}):
                self.doc.pop(field, None)
            for field, value in update.get("$setOnInsert", {}).items():
                self.doc.setdefault(field, value)
            return Result(matched=1)
        if upsert and self.doc is None:
            self.doc = {}
            self.doc.update(update.get("$set", {}))
            self.doc.update(update.get("$setOnInsert", {}))
            return Result(upserted_id="new")
        return Result()

    def update_many(self, key, update):
        if not self._matches(key):
            return Result()
        for field in update.get("$unset", {}):
            self.doc.pop(field, None)
        self.doc.update(update.get("$set", {}))
        return Result(modified=1)

    def _matches(self, key):
        if self.doc is None:
            return False
        for field, value in key.items():
            if field == "$or":
                if not any(self._matches(option) for option in value):
                    return False
                continue
            if field == "claim":
                exists = "claim" in self.doc
                if value == {"$exists": False} and exists:
                    return False
                if value == {"$exists": True} and not exists:
                    return False
                continue
            if field.startswith("claim."):
                nested = (self.doc.get("claim") or {}).get(field.split(".", 1)[1])
                if isinstance(value, dict) and "$lt" in value:
                    if nested is None or not nested < value["$lt"]:
                        return False
                elif nested != value:
                    return False
                continue
            if self.doc.get(field) != value:
                return False
        return True


def item(tmp_path: Path) -> SilverSourceFile:
    path = tmp_path / "fhv_tripdata_2025-01.parquet"
    path.write_bytes(b"x")
    return SilverSourceFile("fhv", 2025, 1, path, "sha", "bronze", 2, "READY")


def test_silver_registry_claim_and_ready_clear_claim(tmp_path):
    collection = Collection()
    repo = SilverFileRegistryRepository(collection)
    source = item(tmp_path)
    assert repo.claim(source, "silver-run") is True
    assert "claim" in collection.doc
    outcome = SilverFileOutcome(source, "READY", rows_read=2, rows_valid=2)
    outcome.curated_path = "curated"
    outcome.rejected_path = "rejected"
    outcome.master_path = "master"
    outcome.reconciliation_status = "MATCHED"
    outcome.finish()
    repo.mark_ready(outcome, "silver-run")
    assert collection.doc["status"] == "READY"
    assert "claim" not in collection.doc
    assert repo.is_unchanged(source, outputs_exist=True) is True


def test_silver_registry_recovers_finished_owner(tmp_path):
    now = utc_now()
    source = item(tmp_path)
    collection = Collection(
        {
            "service": "fhv",
            "year": 2025,
            "month": 1,
            "claim": {
                "execution_id": "old",
                "claimed_at": now,
                "expires_at": now + timedelta(hours=2),
            },
        }
    )
    repo = SilverFileRegistryRepository(
        collection,
        execution_repository=Executions(
            {"old": {"status": "SUCCESS", "finished_at": now}}
        ),
    )
    assert repo.claim(source, "new") is True
    assert collection.doc["claim"]["execution_id"] == "new"


def test_silver_registry_active_owner_blocks(tmp_path):
    now = utc_now()
    source = item(tmp_path)
    collection = Collection(
        {
            "service": "fhv",
            "year": 2025,
            "month": 1,
            "claim": {
                "execution_id": "old",
                "claimed_at": now,
                "expires_at": now + timedelta(hours=2),
            },
        }
    )
    repo = SilverFileRegistryRepository(
        collection,
        execution_repository=Executions({"old": {"status": "RUNNING"}}),
    )
    assert repo.claim(source, "new") is False
