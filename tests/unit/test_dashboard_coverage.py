from dashboard.components.filters import ScopeSelection, coverage_note


def test_hvfhv_missing_years_are_reported_as_unavailable_not_zero():
    message = coverage_note(
        ScopeSelection(years=[2023, 2024, 2025], months=[1], services=["fhvhv"])
    )

    assert message is not None
    assert "2024" in message
    assert "2025" in message
    assert "no se imputa como cero" in message
