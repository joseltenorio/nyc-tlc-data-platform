from tlc_data_platform.ingestion.expected_periods import generate_expected_periods


def test_project_scope_excludes_pre_2023_periods(app_config):
    periods = generate_expected_periods(app_config, ["yellow", "fhvhv"], 2019, 2019, [1, 2])
    assert all(period.applicable is False for period in periods)


def test_hvfhv_scope_is_limited_to_2023(app_config):
    periods = generate_expected_periods(app_config, ["fhvhv"], 2023, 2024, [1])
    assert periods[0].year == 2023 and periods[0].applicable is True
    assert periods[1].year == 2024 and periods[1].applicable is False


def test_matrix_contains_every_requested_period(app_config):
    periods = generate_expected_periods(
        app_config, ["yellow", "green"], 2025, 2026, [1, 2]
    )
    assert len(periods) == 8
    assert all(period.applicable for period in periods)


def test_default_historical_scope_has_120_applicable_partitions(app_config):
    periods = generate_expected_periods(
        app_config,
        ["yellow", "green", "fhv", "fhvhv"],
        2023,
        2025,
        list(range(1, 13)),
    )
    assert sum(period.applicable for period in periods) == 120


def test_incremental_scope_excludes_hvfhv_after_2023(app_config):
    periods = generate_expected_periods(
        app_config,
        ["yellow", "green", "fhv", "fhvhv"],
        2026,
        2026,
        list(range(1, 13)),
    )
    assert sum(period.applicable for period in periods) == 36
