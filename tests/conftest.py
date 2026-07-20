from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from tlc_data_platform.core.settings import load_config


@pytest.fixture
def app_config(tmp_path: Path):
    config = load_config(Path(__file__).parents[1] / "config")
    storage = replace(
        config.storage,
        bronze_root=tmp_path / "bronze" / "trip_records",
        versions_root=tmp_path / "bronze" / "versions" / "trip_records",
        temporary_root=tmp_path / "tmp",
        manifests_root=tmp_path / "manifests" / "bronze",
        minimum_free_space_bytes=0,
    )
    download = replace(
        config.download,
        minimum_file_size_bytes=8,
        initial_backoff_seconds=0,
        max_backoff_seconds=0,
    )
    silver_storage = replace(
        config.silver.storage,
        silver_root=tmp_path / "silver",
        temporary_root=tmp_path / "tmp" / "silver",
        manifests_root=tmp_path / "manifests" / "silver",
    )
    silver_references = replace(
        config.silver.references,
        bronze_root=tmp_path / "bronze" / "reference",
    )
    silver = replace(
        config.silver,
        storage=silver_storage,
        references=silver_references,
    )
    gold_storage = replace(
        config.gold.storage,
        gold_root=tmp_path / "gold",
        temporary_root=tmp_path / "tmp" / "gold",
        manifests_root=tmp_path / "manifests" / "gold",
    )
    gold = replace(config.gold, storage=gold_storage)
    ml_storage = replace(
        config.ml.storage,
        ml_root=tmp_path / "ml",
        model_root=tmp_path / "models",
        temporary_root=tmp_path / "tmp" / "ml",
        manifests_root=tmp_path / "manifests" / "ml",
    )
    ml = replace(config.ml, storage=ml_storage)
    audit_filesystem = replace(
        config.audit.filesystem,
        root=tmp_path / "audit",
        layer_roots={
            "bronze": storage.bronze_root,
            "silver": silver_storage.silver_root,
            "gold": gold_storage.gold_root,
            "ml": ml_storage.ml_root,
        },
    )
    audit = replace(config.audit, filesystem=audit_filesystem)
    return replace(
        config,
        storage=storage,
        download=download,
        silver=silver,
        gold=gold,
        ml=ml,
        audit=audit,
    )

@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("nyc-tlc-data-platform-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    yield session
    session.stop()