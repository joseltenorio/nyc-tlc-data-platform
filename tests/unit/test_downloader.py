
import pytest

from tlc_data_platform.bronze.models import FileCandidate, RemoteMetadata
from tlc_data_platform.bronze.storage import BronzeStorage
from tlc_data_platform.core.exceptions import DownloadError
from tlc_data_platform.ingestion.downloader import FileDownloader, has_parquet_signature


class FakeResponse:
    def __init__(self, body, content_type="application/octet-stream", status=200):
        self.body = body
        self.status_code = status
        self.headers = {"Content-Type": content_type, "Content-Length": str(len(body))}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)

    def iter_content(self, chunk_size):
        for index in range(0, len(self.body), chunk_size):
            yield self.body[index:index + chunk_size]

    def close(self):
        pass


class FakeHttp:
    def __init__(self, response):
        self.response = response

    def request(self, *args, **kwargs):
        return self.response


def candidate():
    return FileCandidate(
        service="yellow",
        year=2026,
        month=1,
        url="https://example/yellow_tripdata_2026-01.parquet",
        file_name="yellow_tripdata_2026-01.parquet",
        discovery_method="html",
    )


def test_downloads_valid_file_and_calculates_checksum(app_config):
    body = b"PAR1abcdefghPAR1"
    storage = BronzeStorage(app_config.storage)
    downloader = FileDownloader(FakeHttp(FakeResponse(body)), storage, app_config.download)
    result = downloader.download(candidate(), "run", RemoteMetadata(True, content_length=len(body)))
    assert result.path.read_bytes() == body
    assert result.sha256
    assert has_parquet_signature(result.path)


def test_rejects_incomplete_file(app_config):
    storage = BronzeStorage(app_config.storage)
    downloader = FileDownloader(FakeHttp(FakeResponse(b"PAR1")), storage, app_config.download)
    with pytest.raises(DownloadError, match="incompleto"):
        downloader.download(candidate(), "run", RemoteMetadata(True, content_length=4))


def test_rejects_html_even_with_parquet_extension(app_config):
    body = b"<html>error</html>"
    storage = BronzeStorage(app_config.storage)
    downloader = FileDownloader(
        FakeHttp(FakeResponse(body, "text/html")), storage, app_config.download
    )
    with pytest.raises(DownloadError, match="HTML"):
        downloader.download(candidate(), "run", RemoteMetadata(True, content_length=len(body)))


def test_rejects_invalid_parquet_signature(app_config):
    body = b"not-a-parquet-file"
    storage = BronzeStorage(app_config.storage)
    downloader = FileDownloader(FakeHttp(FakeResponse(body)), storage, app_config.download)
    with pytest.raises(DownloadError, match="Firma Parquet"):
        downloader.download(candidate(), "run", RemoteMetadata(True, content_length=len(body)))


def test_rejects_when_disk_space_is_insufficient(app_config, monkeypatch):
    storage = BronzeStorage(app_config.storage)
    monkeypatch.setattr(storage, "free_space_bytes", lambda: 0)
    body = b"PAR1abcdefghPAR1"
    downloader = FileDownloader(FakeHttp(FakeResponse(body)), storage, app_config.download)
    from tlc_data_platform.core.exceptions import InsufficientDiskSpaceError

    with pytest.raises(InsufficientDiskSpaceError):
        downloader.download(candidate(), "run", RemoteMetadata(True, content_length=len(body)))

class SequenceHttp:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def request(self, *args, **kwargs):
        self.calls += 1
        return self.responses.pop(0)


def test_retries_complete_download_after_http_202(app_config, monkeypatch):
    body = b"PAR1abcdefghPAR1"
    http = SequenceHttp(
        [
            FakeResponse(b"<html>pending</html>", "text/html", status=202),
            FakeResponse(body),
        ]
    )
    storage = BronzeStorage(app_config.storage)
    events = []
    monkeypatch.setattr("tlc_data_platform.ingestion.downloader.time.sleep", lambda _: None)
    result = FileDownloader(http, storage, app_config.download).download(
        candidate(),
        "run-retry",
        RemoteMetadata(True, content_length=len(body)),
        attempt_callback=events.append,
    )
    assert http.calls == 2
    assert result.attempt_count == 2
    assert result.retry_count == 1
    assert [event["outcome"] for event in events] == ["RETRY", "SUCCESS"]
    assert events[0]["max_attempts"] == 6
