from tlc_data_platform.core.settings import resolve_silver_selection


def test_silver_config_is_loaded(app_config):
    assert app_config.project.version == "3.0.0"
    assert app_config.project.layer == "bronze-silver-gold-ml"
    assert app_config.silver.storage.datasets["fhvhv"] == "hvfhv_trips"
    assert app_config.silver.execution.build_master is True


def test_silver_historical_selection(app_config):
    selection = resolve_silver_selection(app_config, "silver-historical")
    assert (selection.start_year, selection.end_year) == (2023, 2025)
    assert selection.workers == 1


def test_silver_incremental_selection(app_config):
    selection = resolve_silver_selection(app_config, "silver-incremental")
    assert (selection.start_year, selection.end_year) == (2026, 2026)
