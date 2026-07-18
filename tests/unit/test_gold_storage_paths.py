from tlc_data_platform.gold.storage import GoldStorage


def test_gold_fact_partition_uses_spark_compatible_integer_month(app_config):
    storage = GoldStorage(app_config.gold, app_config.silver.storage, app_config.services)
    path = storage.fact_partition_path("trip_activity", "yellow", 2023, 1)
    assert path.name == "source_month=1"


def test_gold_scope_excludes_hvfhv_2024_and_2026(app_config):
    storage = GoldStorage(
        app_config.gold, app_config.silver.storage, app_config.services
    )
    assert storage.period_in_scope("fhvhv", 2023, 12)
    assert not storage.period_in_scope("fhvhv", 2024, 1)
    assert not storage.period_in_scope("fhvhv", 2026, 1)
    assert storage.period_in_scope("yellow", 2026, 12)
