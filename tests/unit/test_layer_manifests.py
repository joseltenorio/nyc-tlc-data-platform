from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

from tlc_data_platform.gold.manifest import GoldManifestWriter
from tlc_data_platform.ml.manifest import MLManifestWriter


@dataclass(frozen=True)
class Summary:
    execution_id: str
    finished_at: datetime

    def to_dict(self):
        return {
            "execution_id": self.execution_id,
            "finished_at": self.finished_at,
        }


def test_gold_manifest_uses_layer_directory_contract(tmp_path):
    writer = GoldManifestWriter(tmp_path / "gold", "gold-run-1")
    path = writer.write(Summary("gold-run-1", datetime.now(timezone.utc)))

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert path.name == "gold-run-1.json"
    assert payload["manifest_schema_version"] == "1.0"
    assert payload["layer"] == "gold"
    assert payload["summary"]["execution_id"] == "gold-run-1"
    assert not path.with_suffix(".json.part").exists()


def test_ml_manifest_uses_layer_directory_contract(tmp_path):
    writer = MLManifestWriter(tmp_path / "ml", "ml-run-1")
    path = writer.write(Summary("ml-run-1", datetime.now(timezone.utc)))

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert path.name == "ml-run-1.json"
    assert payload["manifest_schema_version"] == "1.0"
    assert payload["layer"] == "ml"
    assert payload["summary"]["execution_id"] == "ml-run-1"
    assert not path.with_suffix(".json.part").exists()


def test_silver_reference_manifest_is_versioned_and_separated(tmp_path):
    from types import SimpleNamespace

    from tlc_data_platform.silver.references import (
        DownloadedReference,
        SilverReferencePipeline,
    )

    pipeline = SilverReferencePipeline.__new__(SilverReferencePipeline)
    pipeline._config = SimpleNamespace(
        silver=SimpleNamespace(
            storage=SimpleNamespace(manifests_root=tmp_path / "silver")
        )
    )
    downloaded = DownloadedReference(
        path=tmp_path / "source.csv",
        sha256="abc123",
        size_bytes=128,
        source_url="https://example.test/source.csv",
    )
    started_at = datetime.now(timezone.utc)

    path = pipeline._write_manifest(
        execution_id="references-run-1",
        refreshed_at=started_at,
        taxi_download=downloaded,
        base_download=downloaded,
        taxi_bronze=tmp_path / "bronze" / "taxi.csv",
        base_bronze=tmp_path / "bronze" / "base.csv",
        taxi_final=tmp_path / "silver" / "taxi_zones",
        base_final=tmp_path / "silver" / "base_lookup",
        taxi_rows=265,
        base_rows=10,
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert path.parent.name == "references"
    assert payload["manifest_schema_version"] == "1.0"
    assert payload["layer"] == "silver"
    assert payload["manifest_type"] == "reference_refresh"
    assert payload["summary"]["datasets_built"] == 2
    assert payload["summary"]["rows_valid"] == 275
    assert payload["references"][0]["rows"] == 265
