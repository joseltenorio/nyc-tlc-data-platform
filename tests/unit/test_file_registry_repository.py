from datetime import timedelta

from tlc_data_platform.audit.file_registry_repository import FileRegistryRepository
from tlc_data_platform.bronze.models import DEFERRED_REMOTE_ACCESS, FileCandidate, FileOutcome, utc_now


class FakeUpdateResult:
    def __init__(self, matched=0, upserted_id=None, modified=0):
        self.matched_count = matched
        self.upserted_id = upserted_id
        self.modified_count = modified


class FakeExecutionRepository:
    def __init__(self, docs=None):
        self.docs = docs or {}

    def get(self, execution_id):
        return self.docs.get(execution_id)


class FakeCollection:
    def __init__(self, doc=None):
        self.doc = doc

    def find_one(self, filter_doc, projection=None):
        if self.doc is None:
            return None
        if any(self.doc.get(key) != value for key, value in filter_doc.items() if "." not in key):
            return None
        if projection == {"_id": 0, "claim": 1}:
            return {"claim": self.doc.get("claim")}
        return self.doc.copy()

    def update_one(self, filter_doc, update, upsert=False):
        key_match = self.doc is None or all(
            self.doc.get(key) == value for key, value in filter_doc.items() if key in {"service", "year", "month"}
        )
        if not key_match and not upsert:
            return FakeUpdateResult()

        if self._matches(filter_doc):
            if self.doc is None:
                self.doc = {}
            self.doc.update(update.get("$set", {}))
            for field in update.get("$unset", {}):
                self.doc.pop(field, None)
            for key, value in update.get("$setOnInsert", {}).items():
                self.doc.setdefault(key, value)
            return FakeUpdateResult(matched=1 if self.doc else 0, upserted_id="new" if upsert else None)
        if upsert and self.doc is None:
            self.doc = {}
            self.doc.update(update.get("$set", {}))
            self.doc.update(update.get("$setOnInsert", {}))
            return FakeUpdateResult(upserted_id="new")
        return FakeUpdateResult()

    def update_many(self, filter_doc, update):
        if not self._matches(filter_doc):
            return FakeUpdateResult(modified=0)
        for field in update.get("$unset", {}):
            self.doc.pop(field, None)
        self.doc.update(update.get("$set", {}))
        return FakeUpdateResult(modified=1)

    def _matches(self, filter_doc):
        if self.doc is None:
            return False
        for key, value in filter_doc.items():
            if key == "$or":
                return any(self._matches(option) for option in value)
            if key == "claim":
                exists = "claim" in self.doc
                if value == {"$exists": False} and exists:
                    return False
                if value == {"$exists": True} and not exists:
                    return False
                continue
            if key == "claim.execution_id":
                if (self.doc.get("claim") or {}).get("execution_id") != value:
                    return False
                continue
            if key == "claim.claimed_at":
                if (self.doc.get("claim") or {}).get("claimed_at") != value:
                    return False
                continue
            if key == "claim.expires_at":
                current = (self.doc.get("claim") or {}).get("expires_at")
                if isinstance(value, dict) and "$lt" in value:
                    if current is None or not current < value["$lt"]:
                        return False
                elif current != value:
                    return False
                continue
            if self.doc.get(key) != value:
                return False
        return True


def candidate():
    return FileCandidate(
        service="fhv",
        year=2019,
        month=9,
        url="https://example/fhv_tripdata_2019-09.parquet",
        file_name="fhv_tripdata_2019-09.parquet",
        discovery_method="html",
    )


def make_repository(doc=None, executions=None):
    return FileRegistryRepository(
        FakeCollection(doc),
        claim_ttl_minutes=180,
        execution_repository=FakeExecutionRepository(executions),
    )


def test_claim_without_existing_claim_succeeds():
    repo = make_repository()
    assert repo.claim(candidate(), "run-new") is True


def test_claim_with_active_owner_blocks():
    now = utc_now()
    repo = make_repository(
        {
            "service": "fhv",
            "year": 2019,
            "month": 9,
            "claim": {
                "execution_id": "run-old",
                "claimed_at": now,
                "expires_at": now + timedelta(minutes=30),
            },
        },
        executions={"run-old": {"status": "RUNNING"}},
    )
    assert repo.claim(candidate(), "run-new") is False


def test_claim_with_expired_owner_is_recovered():
    now = utc_now()
    repo = make_repository(
        {
            "service": "fhv",
            "year": 2019,
            "month": 9,
            "claim": {
                "execution_id": "run-old",
                "claimed_at": now - timedelta(hours=4),
                "expires_at": now - timedelta(minutes=1),
            },
        },
        executions={"run-old": {"status": "RUNNING"}},
    )
    assert repo.claim(candidate(), "run-new") is True


def test_claim_with_finished_owner_is_recovered():
    now = utc_now()
    repo = make_repository(
        {
            "service": "fhv",
            "year": 2019,
            "month": 9,
            "claim": {
                "execution_id": "run-old",
                "claimed_at": now,
                "expires_at": now + timedelta(minutes=30),
            },
        },
        executions={"run-old": {"status": "SUCCESS", "finished_at": now}},
    )
    assert repo.claim(candidate(), "run-new") is True


def test_claim_with_missing_owner_is_recovered():
    now = utc_now()
    repo = make_repository(
        {
            "service": "fhv",
            "year": 2019,
            "month": 9,
            "claim": {
                "execution_id": "run-old",
                "claimed_at": now,
                "expires_at": now + timedelta(minutes=30),
            },
        },
        executions={},
    )
    assert repo.claim(candidate(), "run-new") is True


def test_claim_from_same_execution_is_idempotent():
    now = utc_now()
    repo = make_repository(
        {
            "service": "fhv",
            "year": 2019,
            "month": 9,
            "claim": {
                "execution_id": "run-same",
                "claimed_at": now,
                "expires_at": now + timedelta(minutes=30),
            },
        },
        executions={"run-same": {"status": "RUNNING"}},
    )
    assert repo.claim(candidate(), "run-same") is True


def test_mark_ready_clears_claim():
    repo = make_repository(
        {
            "service": "fhv",
            "year": 2019,
            "month": 9,
            "claim": {"execution_id": "run-old"},
        }
    )
    outcome = FileOutcome(candidate(), "READY")
    outcome.local_path = "data/bronze/trip_records/fhv/year=2019/month=09/fhv_tripdata_2019-09.parquet"
    outcome.sha256 = "sha"
    outcome.bytes_downloaded = 10
    outcome.finish()
    repo.mark_ready(outcome, "run-new")
    assert "claim" not in repo._collection.doc


def test_mark_failed_clears_claim():
    repo = make_repository(
        {
            "service": "fhv",
            "year": 2019,
            "month": 9,
            "claim": {"execution_id": "run-old"},
        }
    )
    outcome = FileOutcome(candidate(), "FAILED")
    outcome.error_type = "RuntimeError"
    outcome.error_message = "boom"
    outcome.finish()
    repo.mark_failed(outcome, "run-new")
    assert "claim" not in repo._collection.doc


def test_mark_deferred_clears_claim():
    repo = make_repository(
        {
            "service": "fhv",
            "year": 2019,
            "month": 9,
            "claim": {"execution_id": "run-old"},
        }
    )
    outcome = FileOutcome(candidate(), DEFERRED_REMOTE_ACCESS)
    outcome.error_type = "DownloadError"
    outcome.error_message = "HTML"
    outcome.finish()
    repo.mark_deferred(outcome, "run-new")
    assert "claim" not in repo._collection.doc


def test_release_claims_for_execution_clears_pending_claim():
    repo = make_repository(
        {
            "service": "fhv",
            "year": 2019,
            "month": 9,
            "claim": {"execution_id": "run-old"},
        }
    )
    assert repo.release_claims_for_execution("run-old") == 1
    assert "claim" not in repo._collection.doc