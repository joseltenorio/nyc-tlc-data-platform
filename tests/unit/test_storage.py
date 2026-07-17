from pathlib import Path

from tlc_data_platform.bronze.models import FileCandidate
from tlc_data_platform.bronze.storage import BronzeStorage
from tlc_data_platform.ingestion.checksum import calculate_sha256


def candidate():
    return FileCandidate(
        service="yellow",
        year=2026,
        month=1,
        url="https://example/yellow_tripdata_2026-01.parquet",
        file_name="yellow_tripdata_2026-01.parquet",
        discovery_method="html",
    )


def test_builds_partitioned_path(app_config):
    storage = BronzeStorage(app_config.storage)
    assert storage.final_path(candidate()).parts[-4:] == (
        "yellow", "year=2026", "month=01", "yellow_tripdata_2026-01.parquet"
    )


def test_calculates_sha256(tmp_path: Path):
    path = tmp_path / "x"
    path.write_bytes(b"abc")
    assert calculate_sha256(path) == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def test_archives_previous_version(app_config):
    storage = BronzeStorage(app_config.storage)
    current = storage.final_path(candidate())
    current.parent.mkdir(parents=True)
    current.write_bytes(b"old")
    temporary = storage.temporary_path(candidate(), "run")
    temporary.parent.mkdir(parents=True)
    temporary.write_bytes(b"new")
    final, archived = storage.promote(temporary, candidate(), "oldsha", "newsha")
    assert final.read_bytes() == b"new"
    assert archived is not None and archived.read_bytes() == b"old"


def test_same_checksum_does_not_archive(app_config):
    storage = BronzeStorage(app_config.storage)
    current = storage.final_path(candidate())
    current.parent.mkdir(parents=True)
    current.write_bytes(b"old")
    temporary = storage.temporary_path(candidate(), "run")
    temporary.parent.mkdir(parents=True)
    temporary.write_bytes(b"same")
    final, archived = storage.promote(temporary, candidate(), "sha", "sha")
    assert archived is None
    assert final.read_bytes() == b"same"
