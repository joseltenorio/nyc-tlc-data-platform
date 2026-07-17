import json

from tlc_data_platform.bronze.manifest import ManifestWriter
from tlc_data_platform.bronze.models import ExecutionSummary, utc_now


def test_manifest_is_written_atomically(tmp_path):
    now = utc_now()
    writer = ManifestWriter(tmp_path, "run-1")
    summary = ExecutionSummary(
        execution_id="run-1",
        execution_type="TEST",
        status="SUCCESS",
        started_at=now,
        finished_at=now,
        requested_services=["yellow"],
        requested_start_year=2026,
        requested_end_year=2026,
        requested_months=[1],
        expected_periods=1,
        applicable_periods=1,
        available_files=1,
        downloaded_files=1,
        ready_files=1,
        skipped_files=0,
        failed_files=0,
        failed_probe_periods=0,
        not_published_files=0,
        not_applicable_periods=0,
        total_bytes_downloaded=10,
        manifest_path=str(writer.path),
    )
    writer.write(summary)
    payload = json.loads(writer.path.read_text(encoding="utf-8"))
    assert payload["summary"]["execution_id"] == "run-1"
    assert not writer.path.with_suffix(".json.tmp").exists()
