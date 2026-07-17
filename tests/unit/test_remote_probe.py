from tlc_data_platform.bronze.models import DEFERRED_REMOTE_ACCESS, classify_remote_availability
from tlc_data_platform.ingestion.remote_probe import RemoteProbe


class FakeResponse:
    def __init__(self, status_code, headers=None):
        self.status_code = status_code
        self.headers = headers or {}

    def close(self):
        pass


class FakeHttp:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


def test_head_200_is_available():
    probe = RemoteProbe(
        FakeHttp([FakeResponse(200, {"Content-Length": "8", "Content-Type": "application/octet-stream"})])
    )
    remote = probe.probe("https://example/file.parquet")
    assert remote.available is True
    assert classify_remote_availability(remote) == "AVAILABLE"


def test_head_403_and_range_206_is_available():
    probe = RemoteProbe(
        FakeHttp(
            [
                FakeResponse(403),
                FakeResponse(206, {"Content-Length": "8", "Content-Type": "application/octet-stream"}),
            ]
        )
    )
    remote = probe.probe("https://example/file.parquet")
    assert remote.available is True


def test_head_405_and_range_206_is_available():
    probe = RemoteProbe(
        FakeHttp(
            [
                FakeResponse(405),
                FakeResponse(206, {"Content-Length": "8", "Content-Type": "application/octet-stream"}),
            ]
        )
    )
    remote = probe.probe("https://example/file.parquet")
    assert remote.available is True


def test_404_is_not_published():
    probe = RemoteProbe(FakeHttp([FakeResponse(404)]))
    remote = probe.probe("https://example/file.parquet")
    assert classify_remote_availability(remote) == "NOT_PUBLISHED_YET"


def test_410_is_not_published():
    probe = RemoteProbe(FakeHttp([FakeResponse(410)]))
    remote = probe.probe("https://example/file.parquet")
    assert classify_remote_availability(remote) == "NOT_PUBLISHED_YET"


def test_202_html_is_deferred():
    probe = RemoteProbe(
        FakeHttp(
            [
                FakeResponse(202, {"Content-Type": "text/html; charset=UTF-8"}),
                FakeResponse(202, {"Content-Type": "text/html; charset=UTF-8"}),
            ]
        )
    )
    remote = probe.probe("https://example/file.parquet")
    assert classify_remote_availability(remote) == DEFERRED_REMOTE_ACCESS


def test_persistent_403_is_deferred():
    probe = RemoteProbe(FakeHttp([FakeResponse(403), FakeResponse(403)]))
    remote = probe.probe("https://example/file.parquet")
    assert classify_remote_availability(remote) == DEFERRED_REMOTE_ACCESS


def test_429_is_deferred():
    probe = RemoteProbe(FakeHttp([FakeResponse(429)]))
    remote = probe.probe("https://example/file.parquet")
    assert classify_remote_availability(remote) == DEFERRED_REMOTE_ACCESS


def test_503_is_deferred():
    probe = RemoteProbe(FakeHttp([FakeResponse(503)]))
    remote = probe.probe("https://example/file.parquet")
    assert classify_remote_availability(remote) == DEFERRED_REMOTE_ACCESS