from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import replace
from pathlib import Path

from tlc_data_platform.audit.summaries import AuditRepositories
from tlc_data_platform.bronze.models import (
    AvailabilityRecord,
    DEFERRED_REMOTE_ACCESS,
    DiscoveryResult,
    DownloadResult,
    ExpectedPeriod,
    FileCandidate,
    RemoteMetadata,
    ValidationResult,
)
from tlc_data_platform.bronze.pipeline import BronzePipeline
from tlc_data_platform.bronze.storage import BronzeStorage
from tlc_data_platform.core.exceptions import DownloadError
from tlc_data_platform.core.settings import resolve_selection


class DummyHttp:
    def close(self):
        pass


class FakeProbe:
    def __init__(self, metadata=None):
        self.metadata = metadata or RemoteMetadata(
            True,
            status_code=200,
            content_length=16,
            etag="etag-new",
        )

    def probe(self, url):
        return self.metadata


class FakeDiscovery:
    def __init__(self, candidates):
        self.candidates = candidates

    def discover(self, execution_id, services, start_year, end_year, months):
        expected = [
            ExpectedPeriod(c.service, c.year, c.month, True) for c in self.candidates
        ]
        availability = [
            AvailabilityRecord(
                execution_id=execution_id,
                service=c.service,
                year=c.year,
                month=c.month,
                status="AVAILABLE",
                applicable=True,
                expected=True,
                candidate_url=c.url,
                discovery_method=c.discovery_method,
            )
            for c in self.candidates
        ]
        return DiscoveryResult(expected, self.candidates, availability)


class FakeDownloader:
    def __init__(self, storage, fail_months=(), body=b"PAR1abcdefghPAR1"):
        self.storage = storage
        self.fail_months = set(fail_months)
        self.body = body
        self.calls = 0

    def download(self, candidate, execution_id, remote):
        self.calls += 1
        if candidate.month in self.fail_months:
            raise RuntimeError("download failed")
        path = self.storage.temporary_path(candidate, execution_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.body)
        return DownloadResult(
            candidate=candidate,
            path=path,
            bytes_downloaded=len(self.body),
            sha256=hashlib.sha256(self.body).hexdigest(),
            remote_metadata=remote,
        )


class TrackingDownloader(FakeDownloader):
    def __init__(self, storage):
        super().__init__(storage)
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()

    def download(self, candidate, execution_id, remote):
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            time.sleep(0.03)
            return super().download(candidate, execution_id, remote)
        finally:
            with self.lock:
                self.active -= 1


class FakeValidator:
    def validate(self, path, candidate, spark):
        return ValidationResult(
            expected_required_columns=["pickup_datetime"],
            expected_optional_columns=[],
            observed_columns=["pickup_datetime"],
            missing_required_columns=[],
            missing_optional_columns=[],
            new_columns=[],
            observed_types={"pickup_datetime": "timestamp"},
            required_field_matches={"pickup_datetime": "pickup_datetime"},
            optional_field_matches={},
            type_mismatches={},
            schema_json="{}",
            schema_hash="schema",
            schema_evolution_detected=False,
            schema_events=[],
            parquet_num_rows=1,
            parquet_num_row_groups=1,
            parquet_num_columns=1,
            parquet_created_by="test",
            parquet_compression_codecs=["SNAPPY"],
            sample_rows_read=1,
        )


class FakeSpark:
    def __init__(self):
        self.get_calls = 0

    def get(self):
        self.get_calls += 1
        return object()

    def close(self):
        pass


class FakeExecutions:
    def __init__(self):
        self.started = []
        self.finished = []
        self.failed = []

    def start(self, *args):
        self.started.append(args)

    def finish(self, summary):
        self.finished.append(summary)

    def fail(self, *args):
        self.failed.append(args)

    def get(self, execution_id):
        return None

    def is_active(self, execution_id):
        return False


class FakeAvailability:
    def __init__(self):
        self.records = []

    def insert_many(self, records):
        self.records.extend(records)


class FakeRegistry:
    def __init__(self):
        self.docs = {}
        self.claims = set()
        self.lock = threading.Lock()

    def _key(self, candidate):
        return (candidate.service, candidate.year, candidate.month)

    def get(self, candidate):
        return self.docs.get(self._key(candidate))

    def claim(self, candidate, execution_id):
        key = self._key(candidate)
        with self.lock:
            if key in self.claims:
                return False
            self.claims.add(key)
            return True

    def set_status(self, candidate, execution_id, status, **fields):
        key = self._key(candidate)
        doc = self.docs.setdefault(key, {})
        doc["status"] = status
        doc.update(fields)

    def mark_ready(self, outcome, execution_id):
        key = self._key(outcome.candidate)
        self.docs[key] = {
            "status": "READY",
            "current": {
                "status": "READY",
                "sha256": outcome.sha256,
                "bytes_downloaded": outcome.bytes_downloaded,
                "local_path": outcome.local_path,
                "remote_metadata": outcome.remote_metadata.to_dict(),
            },
        }
        self.claims.discard(key)

    def mark_failed(self, outcome, execution_id):
        self.docs[self._key(outcome.candidate)] = {"status": "FAILED"}
        self.claims.discard(self._key(outcome.candidate))

    def mark_deferred(self, outcome, execution_id):
        self.docs[self._key(outcome.candidate)] = {"status": outcome.status}
        self.claims.discard(self._key(outcome.candidate))

    def release_claim(self, candidate, execution_id):
        self.claims.discard(self._key(candidate))

    def release_claims_for_execution(self, execution_id):
        released = len(self.claims)
        self.claims.clear()
        return released


class FakeVersions:
    def __init__(self):
        self.current = []
        self.archived = []

    def insert_current(self, outcome, execution_id):
        self.current.append((outcome, execution_id))

    def mark_archived(self, *args):
        self.archived.append(args)


def fake_audit():
    return AuditRepositories(
        executions=FakeExecutions(),
        availability=FakeAvailability(),
        registry=FakeRegistry(),
        versions=FakeVersions(),
    )


def candidate(month=1, service="yellow"):
    return FileCandidate(
        service=service,
        year=2026,
        month=month,
        url=f"https://example/{service}_tripdata_2026-{month:02d}.parquet",
        file_name=f"{service}_tripdata_2026-{month:02d}.parquet",
        discovery_method="html",
    )


def make_pipeline(app_config, candidates, audit=None, downloader=None, probe=None):
    storage = BronzeStorage(app_config.storage)
    audit = audit or fake_audit()
    downloader = downloader or FakeDownloader(storage)
    return BronzePipeline(
        app_config,
        http=DummyHttp(),
        probe=probe or FakeProbe(),
        discovery=FakeDiscovery(candidates),
        storage=storage,
        downloader=downloader,
        validator=FakeValidator(),
        spark=FakeSpark(),
        audit=audit,
    ), audit, downloader, storage


def test_pipeline_publishes_file_and_manifest(app_config):
    pipeline, audit, downloader, storage = make_pipeline(app_config, [candidate()])
    selection = resolve_selection(
        app_config, mode="run", services=["yellow"], start_year=2026, end_year=2026, months=[1]
    )
    summary = pipeline.run(selection, execution_type="run")
    assert summary.status == "SUCCESS"
    assert summary.ready_files == 1
    assert storage.final_path(candidate()).read_bytes() == downloader.body
    assert Path(summary.manifest_path).is_file()
    assert audit.executions.finished


def test_dry_run_does_not_download_or_create_spark(app_config):
    pipeline, audit, downloader, _ = make_pipeline(app_config, [candidate()])
    spark = pipeline._spark
    selection = resolve_selection(app_config, mode="incremental", services=["yellow"], months=[1])
    summary = pipeline.run(selection, execution_type="incremental", dry_run=True)
    assert summary.status == "SUCCESS"
    assert downloader.calls == 0
    assert spark.get_calls == 0


def test_reeecution_skips_unchanged_file(app_config):
    audit = fake_audit()
    pipeline, audit, downloader, storage = make_pipeline(app_config, [candidate()], audit=audit)
    destination = storage.final_path(candidate())
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"existing")
    audit.registry.docs[("yellow", 2026, 1)] = {
        "current": {
            "status": "READY",
            "sha256": "abc",
            "bytes_downloaded": len(b"existing"),
            "remote_metadata": {"etag": "etag-new", "last_modified": None, "content_length": 16},
        }
    }
    selection = resolve_selection(app_config, mode="incremental", services=["yellow"], months=[1])
    summary = pipeline.run(selection, execution_type="incremental")
    assert summary.skipped_files == 1
    assert downloader.calls == 0


def test_checksum_change_archives_previous_file(app_config):
    audit = fake_audit()
    probe = FakeProbe(RemoteMetadata(True, 200, 16, etag="etag-new"))
    fallback_candidate = replace(candidate(), discovery_method="deterministic_fallback")
    pipeline, audit, downloader, storage = make_pipeline(
        app_config, [fallback_candidate], audit=audit, probe=probe
    )
    destination = storage.final_path(fallback_candidate)
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"old")
    audit.registry.docs[("yellow", 2026, 1)] = {
        "current": {
            "status": "READY",
            "sha256": "oldsha",
            "bytes_downloaded": 3,
            "remote_metadata": {"etag": "etag-old"},
        }
    }
    selection = resolve_selection(app_config, mode="incremental", services=["yellow"], months=[1])
    summary = pipeline.run(selection, execution_type="incremental")
    assert summary.ready_files == 1
    archived = list(app_config.storage.versions_root.rglob("*.parquet"))
    assert len(archived) == 1 and archived[0].read_bytes() == b"old"
    assert audit.versions.archived


def test_partial_success_when_one_download_fails(app_config):
    storage = BronzeStorage(app_config.storage)
    downloader = FakeDownloader(storage, fail_months=[2])
    pipeline, audit, _, _ = make_pipeline(
        app_config, [candidate(1), candidate(2)], downloader=downloader
    )
    selection = resolve_selection(
        app_config, mode="incremental", services=["yellow"], months=[1, 2]
    )
    summary = pipeline.run(selection, execution_type="incremental")
    assert summary.status == "PARTIAL_SUCCESS"
    assert summary.ready_files == 1
    assert summary.failed_files == 1



def test_claimed_period_prevents_false_success(app_config):
    audit = fake_audit()
    blocked = candidate(1)
    available = candidate(2)
    audit.registry.claims.add(audit.registry._key(blocked))
    pipeline, _, downloader, _ = make_pipeline(
        app_config,
        [blocked, available],
        audit=audit,
    )
    selection = resolve_selection(
        app_config,
        mode="incremental",
        services=["yellow"],
        months=[1, 2],
    )

    summary = pipeline.run(selection, execution_type="incremental")

    assert summary.status == "PARTIAL_SUCCESS"
    assert summary.ready_files == 1
    assert summary.skipped_files == 1
    assert downloader.calls == 1


def test_all_claimed_periods_fail_execution(app_config):
    audit = fake_audit()
    blocked = candidate(1)
    audit.registry.claims.add(audit.registry._key(blocked))
    pipeline, _, downloader, _ = make_pipeline(
        app_config,
        [blocked],
        audit=audit,
    )
    selection = resolve_selection(
        app_config,
        mode="incremental",
        services=["yellow"],
        months=[1],
    )

    summary = pipeline.run(selection, execution_type="incremental")

    assert summary.status == "FAILED"
    assert summary.ready_files == 0
    assert summary.skipped_files == 1
    assert downloader.calls == 0

def test_download_phase_collects_all_futures_after_individual_failure(app_config):
    storage = BronzeStorage(app_config.storage)
    downloader = FakeDownloader(storage, fail_months=[2])
    pipeline, _, _, _ = make_pipeline(
        app_config,
        [candidate(1), candidate(2), candidate(3)],
        downloader=downloader,
    )
    selection = resolve_selection(
        app_config,
        mode="incremental",
        services=["yellow"],
        months=[1, 2, 3],
    )
    summary = pipeline.run(selection, execution_type="incremental")
    assert downloader.calls == 3
    assert summary.ready_files == 2
    assert summary.failed_files == 1


def test_plan_does_not_download(app_config):
    pipeline, audit, downloader, _ = make_pipeline(app_config, [candidate()])
    selection = resolve_selection(app_config, mode="plan", services=["yellow"], start_year=2026, end_year=2026, months=[1])
    result = pipeline.plan(selection)
    assert result.available_files == 1
    assert result.pending_files == 1
    assert downloader.calls == 0
    assert not audit.executions.started


def test_hvfhv_download_concurrency_respects_limit(app_config):
    candidates = [candidate(month, "fhvhv") for month in range(1, 5)]
    storage = BronzeStorage(app_config.storage)
    downloader = TrackingDownloader(storage)
    pipeline, audit, _, _ = make_pipeline(
        app_config, candidates, downloader=downloader
    )
    selection = resolve_selection(
        app_config,
        mode="run",
        services=["fhvhv"],
        start_year=2026,
        end_year=2026,
        months=[1, 2, 3, 4],
        workers=4,
        max_hvfhv_workers=2,
    )
    pending = [
        (c, RemoteMetadata(True, 200, 16), None) for c in candidates
    ]
    downloaded, failures = pipeline._download_pending(
        pending, "run", selection, audit
    )
    assert len(downloaded) == 4
    assert failures == []
    assert downloader.max_active <= 2


def test_force_downloads_even_when_remote_metadata_is_unchanged(app_config):
    audit = fake_audit()
    pipeline, audit, downloader, storage = make_pipeline(app_config, [candidate()], audit=audit)
    destination = storage.final_path(candidate())
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"existing")
    audit.registry.docs[("yellow", 2026, 1)] = {
        "current": {
            "status": "READY",
            "sha256": "oldsha",
            "bytes_downloaded": len(b"existing"),
            "remote_metadata": {"etag": "etag-new", "content_length": 16},
        }
    }
    selection = resolve_selection(app_config, mode="incremental", services=["yellow"], months=[1])
    summary = pipeline.run(selection, execution_type="incremental", force=True)
    assert downloader.calls == 1
    assert summary.ready_files == 1


def test_ready_without_physical_file_is_reprocessed(app_config):
    audit = fake_audit()
    pipeline, audit, downloader, _ = make_pipeline(app_config, [candidate()], audit=audit)
    audit.registry.docs[("yellow", 2026, 1)] = {
        "current": {
            "status": "READY",
            "sha256": "oldsha",
            "bytes_downloaded": 16,
            "local_path": "data/bronze/trip_records/yellow/year=2026/month=01/yellow_tripdata_2026-01.parquet",
            "remote_metadata": {"etag": "etag-new", "content_length": 16},
        }
    }
    selection = resolve_selection(app_config, mode="incremental", services=["yellow"], months=[1])
    summary = pipeline.run(selection, execution_type="incremental")
    assert downloader.calls == 1
    assert summary.ready_files == 1


def test_download_failure_releases_claim_and_cleans_temporary(app_config):
    storage = BronzeStorage(app_config.storage)
    downloader = FakeDownloader(storage, fail_months=[1])
    pipeline, audit, _, _ = make_pipeline(
        app_config,
        [candidate()],
        downloader=downloader,
    )
    selection = resolve_selection(app_config, mode="incremental", services=["yellow"], months=[1])
    summary = pipeline.run(selection, execution_type="incremental")
    assert summary.failed_files == 1
    assert audit.registry.claims == set()
    assert list(app_config.storage.temporary_root.glob("*.part")) == []


def test_consecutive_temporary_blocks_defer_remaining_downloads(app_config):
    class BlockingDownloader(FakeDownloader):
        def download(self, candidate, execution_id, remote):
            self.calls += 1
            raise DownloadError("El servidor devolvió HTML en lugar de Parquet")

    storage = BronzeStorage(app_config.storage)
    downloader = BlockingDownloader(storage)
    pipeline, _, _, _ = make_pipeline(
        app_config,
        [candidate(1), candidate(2), candidate(3)],
        downloader=downloader,
    )
    selection = resolve_selection(
        app_config,
        mode="incremental",
        services=["yellow"],
        months=[1, 2, 3],
        workers=1,
    )

    summary = pipeline.run(selection, execution_type="incremental")
    manifest = Path(summary.manifest_path).read_text(encoding="utf-8")

    assert downloader.calls == 2
    assert summary.status == "FAILED"
    assert DEFERRED_REMOTE_ACCESS in manifest


def test_atomic_claim_allows_only_one_worker():
    registry = FakeRegistry()
    target = candidate()
    results = []
    barrier = threading.Barrier(2)

    def claim():
        barrier.wait()
        results.append(registry.claim(target, "run"))

    threads = [threading.Thread(target=claim) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(results) == [False, True]


class FailingMongoProvider:
    def database(self):
        raise RuntimeError("mongo unavailable")

    def close(self):
        pass


def test_mongodb_failure_is_not_hidden(app_config):
    pipeline = BronzePipeline(
        app_config,
        http=DummyHttp(),
        probe=FakeProbe(),
        discovery=FakeDiscovery([candidate()]),
        mongo_provider=FailingMongoProvider(),
    )
    selection = resolve_selection(app_config, mode="incremental", services=["yellow"], months=[1])
    import pytest

    with pytest.raises(RuntimeError, match="mongo unavailable"):
        pipeline.run(selection, execution_type="incremental")


class FailingSpark(FakeSpark):
    def get(self):
        raise RuntimeError("spark unavailable")


def test_spark_failure_marks_execution_failed(app_config):
    audit = fake_audit()
    storage = BronzeStorage(app_config.storage)
    pipeline = BronzePipeline(
        app_config,
        http=DummyHttp(),
        probe=FakeProbe(),
        discovery=FakeDiscovery([candidate()]),
        storage=storage,
        downloader=FakeDownloader(storage),
        validator=FakeValidator(),
        spark=FailingSpark(),
        audit=audit,
    )
    selection = resolve_selection(app_config, mode="incremental", services=["yellow"], months=[1])
    import pytest

    with pytest.raises(RuntimeError, match="spark unavailable"):
        pipeline.run(selection, execution_type="incremental")
    assert audit.executions.failed