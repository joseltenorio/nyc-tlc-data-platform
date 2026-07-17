import requests

from tlc_data_platform.ingestion.http_client import HttpClient


class FakeResponse:
    def __init__(self, status_code, headers=None):
        self.status_code = status_code
        self.headers = headers or {}
        self.closed = False

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, results):
        self.results = list(results)
        self.calls = 0

    def request(self, *args, **kwargs):
        self.calls += 1
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def test_http_429_respects_retry_after(app_config, monkeypatch):
    client = HttpClient(app_config.discovery, app_config.download)
    session = FakeSession([FakeResponse(429, {"Retry-After": "0"}), FakeResponse(200)])
    client._local.session = session
    monkeypatch.setattr("time.sleep", lambda seconds: None)
    response = client.request("GET", "https://example.test/file")
    assert response.status_code == 200
    assert session.calls == 2


def test_timeout_is_retried(app_config, monkeypatch):
    client = HttpClient(app_config.discovery, app_config.download)
    session = FakeSession([requests.Timeout("slow"), FakeResponse(200)])
    client._local.session = session
    monkeypatch.setattr("time.sleep", lambda seconds: None)
    response = client.request("GET", "https://example.test/file")
    assert response.status_code == 200
    assert session.calls == 2


def test_custom_retryable_status_codes_allow_retrying_403(app_config, monkeypatch):
    client = HttpClient(app_config.discovery, app_config.download)
    session = FakeSession([FakeResponse(403), FakeResponse(200)])
    client._local.session = session
    monkeypatch.setattr("time.sleep", lambda seconds: None)
    response = client.request(
        "GET",
        "https://example.test/file",
        retryable_status_codes={403},
    )
    assert response.status_code == 200
    assert session.calls == 2
