from pathlib import Path

from tlc_data_platform.silver.models import SilverSourceFile
from tlc_data_platform.silver.storage import SilverStorage


def source(tmp_path: Path) -> SilverSourceFile:
    input_path = tmp_path / "yellow_tripdata_2025-01.parquet"
    input_path.write_bytes(b"PAR1xPAR1")
    return SilverSourceFile("yellow", 2025, 1, input_path, "sha", "bronze-run", 1, "READY")


def _fake_parquet_dir(path: Path, value: bytes = b"data") -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / "part-00000.parquet").write_bytes(value)
    (path / "_SUCCESS").write_text("", encoding="utf-8")


def test_silver_paths_follow_dataset_and_partitions(app_config, tmp_path):
    storage = SilverStorage(app_config.silver.storage)
    item = source(tmp_path)
    assert storage.curated_partition(item).as_posix().endswith("silver/yellow_trips/year=2025/month=01")
    assert "service_type=yellow" in storage.rejected_partition(item).as_posix()
    assert "trips_master" in storage.master_partition(item).as_posix()


def test_promote_replaces_partition_atomically(app_config, tmp_path):
    storage = SilverStorage(app_config.silver.storage)
    item = source(tmp_path)
    execution_id = "silver-test"
    for kind in ("curated", "rejected", "master"):
        _fake_parquet_dir(storage.temp_partition(execution_id, item, kind), kind.encode())
    old = storage.curated_partition(item)
    _fake_parquet_dir(old, b"old")
    curated, rejected, master = storage.promote(execution_id, item, include_master=True)
    assert (curated / "part-00000.parquet").read_bytes() == b"curated"
    assert (rejected / "part-00000.parquet").read_bytes() == b"rejected"
    assert master is not None and (master / "part-00000.parquet").read_bytes() == b"master"
    assert not curated.with_name("month=01.previous").exists()
