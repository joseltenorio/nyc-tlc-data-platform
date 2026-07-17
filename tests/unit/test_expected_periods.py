from tlc_data_platform.ingestion.expected_periods import generate_expected_periods


def test_hvfhv_january_2019_is_not_applicable(app_config):
    periods = generate_expected_periods(app_config, ["fhvhv"], 2019, 2019, [1, 2])
    assert periods[0].month == 1 and periods[0].applicable is False
    assert periods[1].month == 2 and periods[1].applicable is True


def test_matrix_contains_every_requested_period(app_config):
    periods = generate_expected_periods(
        app_config, ["yellow", "green"], 2025, 2026, [1, 2]
    )
    assert len(periods) == 8
