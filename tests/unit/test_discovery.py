from tlc_data_platform.bronze.models import DEFERRED_REMOTE_ACCESS, RemoteMetadata
from tlc_data_platform.ingestion.discovery import FileDiscovery


class FakeHttp:
    def __init__(self, html=None, error=None):
        self.html = html
        self.error = error

    def get_text(self, url):
        if self.error:
            raise self.error
        return self.html


class FakeProbe:
    def __init__(self, available_urls=(), failed_urls=(), deferred_urls=()):
        self.available_urls = set(available_urls)
        self.failed_urls = set(failed_urls)
        self.deferred_urls = set(deferred_urls)
        self.calls = []

    def probe(self, url):
        self.calls.append(url)
        if url in self.failed_urls:
            return RemoteMetadata(False, probe_failed=True, error_message="timeout")
        if url in self.deferred_urls:
            return RemoteMetadata(
                False,
                status_code=202,
                content_type="text/html; charset=UTF-8",
            )
        return RemoteMetadata(url in self.available_urls, status_code=200 if url in self.available_urls else 404)


def test_html_complete_does_not_probe_fallback(app_config):
    html = '<a href="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet">x</a>'
    probe = FakeProbe()
    discovery = FileDiscovery(app_config, FakeHttp(html), probe)
    result = discovery.discover("x", ["yellow"], 2026, 2026, [1])
    assert len(result.candidates) == 1
    assert result.candidates[0].discovery_method == "html"
    assert probe.calls == []


def test_html_partial_is_completed_by_fallback(app_config):
    html = '<a href="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet">x</a>'
    fallback_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-02.parquet"
    probe = FakeProbe([fallback_url])
    discovery = FileDiscovery(app_config, FakeHttp(html), probe)
    result = discovery.discover("x", ["yellow"], 2026, 2026, [1, 2])
    assert [candidate.month for candidate in result.candidates] == [1, 2]
    assert result.candidates[1].discovery_method == "deterministic_fallback"
    assert probe.calls == [fallback_url]


def test_html_failure_uses_fallback(app_config):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2026-01.parquet"
    discovery = FileDiscovery(app_config, FakeHttp(error=TimeoutError("offline")), FakeProbe([url]))
    result = discovery.discover("x", ["green"], 2026, 2026, [1])
    assert result.html_error
    assert result.availability[0].status == "AVAILABLE"


def test_unpublished_url_is_recorded(app_config):
    discovery = FileDiscovery(app_config, FakeHttp(""), FakeProbe())
    result = discovery.discover("x", ["yellow"], 2026, 2026, [12])
    assert result.availability[0].status == "NOT_PUBLISHED_YET"


def test_failed_probe_is_distinguished(app_config):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-12.parquet"
    discovery = FileDiscovery(app_config, FakeHttp(""), FakeProbe(failed_urls=[url]))
    result = discovery.discover("x", ["yellow"], 2026, 2026, [12])
    assert result.availability[0].status == "FAILED_TO_PROBE"


def test_fallback_temporary_remote_access_is_deferred(app_config):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-12.parquet"
    discovery = FileDiscovery(app_config, FakeHttp(""), FakeProbe(deferred_urls=[url]))
    result = discovery.discover("x", ["yellow"], 2026, 2026, [12])
    assert result.availability[0].status == DEFERRED_REMOTE_ACCESS


def test_html_candidate_is_not_downgraded_by_probe(app_config):
    html = '<a href="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet">x</a>'
    discovery = FileDiscovery(app_config, FakeHttp(html), FakeProbe())
    result = discovery.discover("x", ["yellow"], 2026, 2026, [1])
    assert result.availability[0].status == "AVAILABLE"
    assert result.candidates[0].discovery_method == "html"