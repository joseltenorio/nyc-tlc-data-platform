import json

from tlc_data_platform.silver.manifest import SilverManifestWriter
from tlc_data_platform.silver.models import (
    SilverExecutionSummary,
    SilverFileOutcome,
    SilverPeriodState,
    SilverSourceFile,
    utc_now,
)


def test_silver_manifest_contains_reconciliation(tmp_path):
    source_path = tmp_path / "x.parquet"
    source_path.write_bytes(b"x")
    source = SilverSourceFile("green", 2025, 1, source_path, "sha", "bronze", 3, "READY")
    outcome = SilverFileOutcome(source, "READY", rows_read=3, rows_valid=2, rows_rejected=1)
    outcome.reconciliation_status = "MATCHED"
    outcome.finish()
    writer = SilverManifestWriter(tmp_path, "silver-1")
    writer.set_states([SilverPeriodState("green", 2025, 1, "BRONZE_READY")])
    writer.add(outcome)
    now = utc_now()
    summary = SilverExecutionSummary(
        "silver-1", "historical", "SUCCESS", now, now,
        ["green"], 2025, 2025, [1], 1, 1, 0, 0, 3, 2, 1, 0,
        str(writer.path),
    )
    writer.write(summary)
    payload = json.loads(writer.path.read_text())
    assert payload["files"][0]["reconciliation_status"] == "MATCHED"
    assert payload["summary"]["rows_rejected"] == 1
