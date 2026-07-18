import json

import pytest

pyspark = pytest.importorskip("pyspark")

from tlc_data_platform.silver.references import SilverReferencePipeline  # noqa: E402


class Response:
    def __init__(self, content: bytes):
        self.content = content

    def raise_for_status(self):
        return None


class Session:
    def __init__(self, payloads):
        self.payloads = payloads
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return Response(self.payloads[url])

    def close(self):
        pass


class StaticSparkProvider:
    def __init__(self, spark):
        self.spark = spark

    def get(self):
        return self.spark

    def close(self):
        pass


def test_references_land_in_bronze_and_publish_silver(spark, app_config):
    zone_lines = ["LocationID,Borough,Zone,service_zone"]
    for location_id in range(1, 266):
        zone_lines.append(
            f'{location_id},Queens,"Zone {location_id}",Boro Zone'
        )
    zone_csv = ("\n".join(zone_lines) + "\n").encode()
    base_csv = (
        "base_license,base_name,dba,license_type,base_telephone_number\n"
        "B01234,TEST BASE,TEST DBA,Black Car,2125550100\n"
    ).encode()
    session = Session(
        {
            app_config.silver.references.taxi_zones_url: zone_csv,
            app_config.silver.references.base_lookup_url: base_csv,
        }
    )
    pipeline = SilverReferencePipeline(
        app_config,
        session=session,
        spark_provider=StaticSparkProvider(spark),
    )
    result = pipeline.run()
    assert result.status == "SUCCESS"
    assert result.taxi_zones_rows == 265
    assert result.base_lookup_rows == 1
    assert "sha256=" in result.taxi_zones_bronze_path
    assert "sha256=" in result.base_lookup_bronze_path
    assert app_config.silver.storage.silver_root.joinpath("taxi_zones").is_dir()
    assert app_config.silver.storage.silver_root.joinpath("base_lookup").is_dir()
    manifest = json.loads(open(result.manifest_path, encoding="utf-8").read())
    assert manifest["references"][0]["rows"] == 265
    assert manifest["references"][1]["sha256"] == result.base_lookup_sha256
