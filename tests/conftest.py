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
        manifests_root=tmp_path / "manifests",
        minimum_free_space_bytes=0,
    )
    download = replace(
        config.download,
        minimum_file_size_bytes=8,
        initial_backoff_seconds=0,
        max_backoff_seconds=0,
    )
    return replace(config, storage=storage, download=download)
