from tlc_data_platform.core.settings import resolve_gold_selection


def test_gold_and_ml_configs_are_loaded(app_config):
    assert app_config.gold.datasets.facts["trip_activity"] == "fact_trip_activity"
    assert app_config.gold.datasets.ml_features["zone_hourly_demand"] == "ml_zone_hourly_demand_features"
    assert app_config.ml.forecast.feature_dataset == "ml_zone_hourly_demand_features"
    assert app_config.ml.wait_risk.excessive_wait_threshold_seconds == 600
    assert "wait-risk" not in app_config.ml.forecast.algorithms


def test_gold_historical_selection_uses_2023_to_2025(app_config):
    selection = resolve_gold_selection(app_config, "gold-historical")
    assert (selection.start_year, selection.end_year) == (2023, 2025)
