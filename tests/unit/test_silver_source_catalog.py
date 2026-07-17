from pathlib import Path

from tlc_data_platform.core.settings import resolve_silver_selection
from tlc_data_platform.silver.source_catalog import SilverSourceCatalog


class FakeCollection:
    def __init__(self, docs):
        self.docs = docs

    def find_one(self, key, projection=None):
        return self.docs.get((key["service"], key["year"], key["month"]))


def test_catalog_uses_only_ready_bronze_registry(app_config, tmp_path):
    path = tmp_path / "yellow_tripdata_2025-01.parquet"
    path.write_bytes(b"PAR1xPAR1")
    docs = {
        ("yellow", 2025, 1): {
            "status": "READY",
            "current": {
                "local_path": str(path),
                "sha256": "abc",
                "execution_id": "bronze-1",
                "validation": {"parquet_num_rows": 10},
            },
        },
        ("yellow", 2025, 2): {"status": "FAILED"},
    }
    selection = resolve_silver_selection(
        app_config,
        "silver-run",
        services=["yellow"],
        start_year=2025,
        end_year=2025,
        months=[1, 2],
    )
    sources, states = SilverSourceCatalog(app_config, FakeCollection(docs)).list(selection)
    assert len(sources) == 1
    assert sources[0].source_sha256 == "abc"
    assert sources[0].bronze_num_rows == 10
    assert [state.status for state in states] == ["BRONZE_READY", "BRONZE_NOT_READY"]


def test_catalog_marks_hvfhv_january_2019_not_applicable(app_config):
    selection = resolve_silver_selection(
        app_config,
        "silver-run",
        services=["fhvhv"],
        start_year=2019,
        end_year=2019,
        months=[1],
    )
    sources, states = SilverSourceCatalog(app_config, FakeCollection({})).list(selection)
    assert sources == []
    assert states[0].status == "NOT_APPLICABLE"
