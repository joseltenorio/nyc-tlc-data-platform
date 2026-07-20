from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from tlc_data_platform.audit.parquet_metrics import parquet_metrics
from tlc_data_platform.audit.unified import UnifiedAuditRepository
from tlc_data_platform.ingestion.http_client import HttpClient


class FakeCollection:
    def __init__(self):
        self.documents = []

    def create_index(self, *args, **kwargs):
        return kwargs.get("name")

    def update_one(self, filter_doc, update, upsert=False):
        document = next(
            (
                item
                for item in self.documents
                if all(item.get(key) == value for key, value in filter_doc.items())
            ),
            None,
        )
        if document is None:
            document = dict(filter_doc)
            self.documents.append(document)
        document.update(update.get("$setOnInsert", {}))
        document.update(update.get("$set", {}))

    def insert_one(self, document):
        self.documents.append(dict(document))

    def find_one(self, filter_doc, projection=None):
        return next(
            (
                dict(item)
                for item in self.documents
                if all(item.get(key) == value for key, value in filter_doc.items())
            ),
            None,
        )


class FakeDatabase:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        return self.collections.setdefault(name, FakeCollection())


class FakeSession:
    def __init__(self, results):
        self.results = list(results)
        self.calls = 0

    def request(self, *args, **kwargs):
        self.calls += 1
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def test_bronze_is_configured_for_five_retries(app_config):
    assert app_config.download.max_retries == 5


def test_http_audit_records_all_six_attempts_after_five_retries(app_config, monkeypatch):
    client = HttpClient(app_config.discovery, app_config.download)
    client._local.session = FakeSession([requests.Timeout("slow")] * 6)
    events = []
    monkeypatch.setattr("time.sleep", lambda seconds: None)

    with pytest.raises(requests.Timeout):
        client.request("GET", "https://example.test/file", attempt_callback=events.append)

    assert len(events) == 6
    assert [event["attempt_number"] for event in events] == [1, 2, 3, 4, 5, 6]
    assert events[-1]["outcome"] == "EXHAUSTED"
    assert events[-1]["max_attempts"] == 6


def test_parquet_metrics_use_metadata_without_spark(tmp_path: Path):
    destination = tmp_path / "dataset"
    destination.mkdir()
    pq.write_table(pa.table({"value": [1, 2, 3]}), destination / "part-000.parquet")

    metrics = parquet_metrics(destination)

    assert metrics.parquet_files == 1
    assert metrics.rows == 3
    assert metrics.bytes_on_disk > 0


def test_coverage_excludes_not_published_when_configured(app_config):
    database = FakeDatabase()
    repository = UnifiedAuditRepository(database, app_config.audit)

    repository.record_coverage(
        "run-1",
        layer="bronze",
        expected_count=4,
        available_count=3,
        ready_count=3,
        missing=[],
        not_published_count=1,
    )

    collection = database[app_config.audit.collections.coverage_snapshots]
    document = collection.documents[0]
    assert document["status"] == "COMPLETE"
    assert document["coverage_rate"] == 1.0
    assert document["not_published_count"] == 1


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_unified_audit_writes_mongo_and_jsonl_with_real_metrics(app_config):
    database = FakeDatabase()
    audit = UnifiedAuditRepository(database, app_config.audit)

    audit.start_run("run-jsonl", layer="bronze", execution_type="historical")
    audit.record_dataset(
        "run-jsonl",
        layer="bronze",
        dataset_name="yellow-2025-01",
        dataset_type="trip_records",
        operation="publish",
        status="READY",
        path="data/bronze/trip_records/yellow/year=2025/month=01",
        parquet_files=1,
        rows=100,
        bytes_on_disk=2048,
    )
    audit.record_download_attempt(
        "run-jsonl",
        service="yellow",
        year=2025,
        month=1,
        url="https://example.test/yellow.parquet",
        attempt_number=1,
        max_attempts=6,
        outcome="SUCCESS",
        duration_seconds=2.0,
        bytes_downloaded=2048,
        throughput_bytes_per_second=1024.0,
    )
    audit.finish_run(
        "run-jsonl",
        status="SUCCESS",
        metrics={"download_seconds": 2.0, "error_rate": 0.0},
    )

    root = app_config.audit.filesystem.root
    run_events = read_jsonl(root / "bronze" / "pipeline_runs.jsonl")
    datasets = read_jsonl(root / "bronze" / "dataset_events.jsonl")
    attempts = read_jsonl(root / "bronze" / "download_attempts.jsonl")

    assert [event["event_action"] for event in run_events] == ["START", "FINISH"]
    assert datasets[0]["parquet_files"] == 1
    assert datasets[0]["rows"] == 100
    assert attempts[0]["duration_seconds"] == 2.0
    assert attempts[0]["throughput_bytes_per_second"] == 1024.0
    assert database[app_config.audit.collections.dataset_events].documents[0]["rows"] == 100


def test_link_parent_preserves_child_layer_across_repository_instances(app_config):
    database = FakeDatabase()
    child_repository = UnifiedAuditRepository(database, app_config.audit)
    child_repository.start_run("child-1", layer="silver", execution_type="run")

    orchestrator_repository = UnifiedAuditRepository(database, app_config.audit)
    orchestrator_repository.link_parent("child-1", "platform-1")

    root = app_config.audit.filesystem.root
    records = read_jsonl(root / "silver" / "pipeline_runs.jsonl")
    assert records[-1]["event_action"] == "LINK_PARENT"
    assert records[-1]["layer"] == "silver"
    assert not (root / "unknown" / "pipeline_runs.jsonl").exists()


def test_coverage_without_applicable_scope_is_not_reported_as_one_hundred_percent(
    app_config,
):
    database = FakeDatabase()
    repository = UnifiedAuditRepository(database, app_config.audit)

    repository.record_coverage(
        "run-no-scope",
        layer="gold",
        expected_count=2,
        available_count=0,
        ready_count=0,
        missing=[],
        not_applicable_count=2,
    )

    collection = database[app_config.audit.collections.coverage_snapshots]
    document = collection.documents[0]
    assert document["status"] == "NO_SCOPE"
    assert document["coverage_rate"] is None
