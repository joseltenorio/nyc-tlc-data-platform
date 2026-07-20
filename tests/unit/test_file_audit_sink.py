from __future__ import annotations

import json
from pathlib import Path

from tlc_data_platform.audit.file_sink import FileAuditSink
from tlc_data_platform.core.settings import AuditFilesystemConfig


def filesystem_config(tmp_path: Path) -> AuditFilesystemConfig:
    layer_roots = {
        layer: tmp_path / "data" / layer
        for layer in ("bronze", "silver", "gold", "ml")
    }
    return AuditFilesystemConfig(
        enabled=True,
        root=tmp_path / "audit",
        pipeline_runs_file="pipeline_runs.jsonl",
        dataset_events_file="dataset_events.jsonl",
        quality_events_file="quality_events.jsonl",
        coverage_snapshots_file="coverage_snapshots.jsonl",
        download_attempts_file="download_attempts.jsonl",
        inventory_snapshots_file="inventory_snapshots.jsonl",
        inventory_current_file="medallion_inventory.json",
        layer_roots=layer_roots,
    )


def test_append_writes_one_real_json_object_per_line(tmp_path: Path):
    sink = FileAuditSink(filesystem_config(tmp_path))

    sink.append(
        "dataset_event",
        "bronze",
        {
            "event_id": "event-1",
            "execution_id": "run-1",
            "dataset_name": "yellow-2025-01",
            "parquet_files": 1,
            "bytes_on_disk": 8,
        },
    )

    path = tmp_path / "audit" / "bronze" / "dataset_events.jsonl"
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["event_id"] == "event-1"
    assert payload["layer"] == "bronze"
    assert payload["parquet_files"] == 1
    assert payload["bytes_on_disk"] == 8
    assert payload["audit_schema_version"] == "1.0"


def test_inventory_counts_physical_parquet_by_layer(tmp_path: Path):
    config = filesystem_config(tmp_path)
    bronze_file = (
        config.layer_roots["bronze"]
        / "trip_records"
        / "yellow"
        / "year=2025"
        / "part.parquet"
    )
    bronze_reference = (
        config.layer_roots["bronze"] / "reference" / "taxi_zones" / "part.parquet"
    )
    silver_file = config.layer_roots["silver"] / "trips_curated" / "part.parquet"
    gold_dimension = (
        config.layer_roots["gold"] / "dimensions" / "dim_date" / "part.parquet"
    )
    gold_fact = config.layer_roots["gold"] / "facts" / "fact_trips" / "part.parquet"
    for file_path in (
        bronze_file,
        bronze_reference,
        silver_file,
        gold_dimension,
        gold_fact,
    ):
        file_path.parent.mkdir(parents=True, exist_ok=True)
    bronze_file.write_bytes(b"PAR1")
    bronze_reference.write_bytes(b"ZONE")
    silver_file.write_bytes(b"PAR1DATA")
    gold_dimension.write_bytes(b"DATE")
    gold_fact.write_bytes(b"FACT")

    snapshot = FileAuditSink(config).refresh_inventory(
        execution_id="run-1",
        trigger_layer="silver",
        status="SUCCESS",
    )

    assert snapshot is not None
    by_layer = {item["layer"]: item for item in snapshot["layers"]}
    assert by_layer["bronze"]["parquet_files"] == 2
    assert by_layer["bronze"]["bytes_on_disk"] == 8
    assert {item["dataset_name"] for item in by_layer["bronze"]["datasets"]} == {
        "yellow",
        "reference/taxi_zones",
    }
    assert by_layer["silver"]["parquet_files"] == 1
    assert by_layer["silver"]["bytes_on_disk"] == 8
    assert by_layer["gold"]["parquet_files"] == 2
    assert by_layer["gold"]["dataset_count"] == 2
    assert {item["dataset_name"] for item in by_layer["gold"]["datasets"]} == {
        "dim_date",
        "fact_trips",
    }
    assert snapshot["totals"]["parquet_files"] == 5

    current = json.loads(
        (tmp_path / "audit" / "inventory" / "medallion_inventory.json").read_text(
            encoding="utf-8"
        )
    )
    history = (
        tmp_path / "audit" / "inventory" / "inventory_snapshots.jsonl"
    ).read_text(encoding="utf-8").splitlines()
    assert current["execution_id"] == "run-1"
    assert len(history) == 1
