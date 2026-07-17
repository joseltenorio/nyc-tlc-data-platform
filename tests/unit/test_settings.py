from dataclasses import replace

import pytest

from tlc_data_platform.core.exceptions import ConfigurationError
from tlc_data_platform.core.settings import resolve_selection


def test_default_periods_start_in_2019(app_config):
    assert app_config.period.historical_start_year == 2019
    assert app_config.period.historical_end_year == 2025
    assert app_config.period.incremental_year == 2026


def test_historical_selection_uses_2019_to_2025(app_config):
    selection = resolve_selection(app_config, mode="historical")
    assert (selection.start_year, selection.end_year) == (2019, 2025)


def test_incremental_selection_uses_2026(app_config):
    selection = resolve_selection(app_config, mode="incremental")
    assert (selection.start_year, selection.end_year) == (2026, 2026)


def test_worker_limits_are_validated(app_config):
    with pytest.raises(ConfigurationError):
        resolve_selection(app_config, mode="run", workers=2, max_hvfhv_workers=3)


def test_disabled_service_is_rejected(app_config):
    services = dict(app_config.services)
    services["green"] = replace(services["green"], enabled=False)
    config = replace(app_config, services=services)
    with pytest.raises(ConfigurationError, match="deshabilitados"):
        resolve_selection(config, mode="run", services=["green"])
