import json
from pathlib import Path

import pytest

from telemetryd.ext.json_reporter import JSONReporter
from telemetryd.metrics import SNMPResponse


class DummyResponse(SNMPResponse):
    """Minimal SNMPResponse stand‑in."""

    def __init__(self, oid="1.2.3", name="metric", value=42, snmp_type="gauge"):
        self.oid = oid
        self.name = name
        self.value = value
        self.snmp_type = snmp_type


@pytest.fixture
def tmp_json(tmp_path):
    """Create a temporary JSONReporter pointing to a fresh file."""
    path = tmp_path / "out" / "metrics.json"
    reporter = JSONReporter(path)
    yield reporter, path
    reporter.close()


def read_lines(path: Path):
    with path.open() as fh:
        return [json.loads(line) for line in fh if line.strip()]


def test_directory_created(tmp_path):
    path = tmp_path / "nested" / "deep" / "metrics.json"
    reporter = JSONReporter(path)
    reporter.close()
    assert path.exists()


def test_file_starts_empty(tmp_json):
    reporter, path = tmp_json
    assert read_lines(path) == []


def test_startup_event(tmp_json):
    reporter, path = tmp_json

    reporter.startup(device_count=5, interval=2.5)
    rows = read_lines(path)
    row = rows[0]

    assert row["event"] == "startup"
    assert row["device_count"] == 5
    assert row["interval"] == 2.5
    assert isinstance(row["timestamp"], float)
    assert row["timestamp"] > 0


@pytest.mark.parametrize(
    "value,rate",
    [
        (123, 1.5),
        ("abc", 0.0),
        (None, 9.9),
    ],
)
def test_metric_event(tmp_json, value, rate):
    reporter, path = tmp_json

    resp = DummyResponse(value=value)
    reporter.metric("host1", resp, rate)

    row = read_lines(path)[0]

    assert row["event"] == "metric"
    assert row["host"] == "host1"
    assert row["oid"] == resp.oid
    assert row["name"] == resp.name
    assert row["value"] == value
    assert row["rate"] == rate
    assert row["snmp_type"] == resp.snmp_type
    assert isinstance(row["timestamp"], float)


def test_init_value_event(tmp_json):
    reporter, path = tmp_json

    resp = DummyResponse(value=999)
    reporter.init_value("routerA", resp)

    row = read_lines(path)[0]

    assert row["event"] == "init"
    assert row["host"] == "routerA"
    assert row["value"] == 999
    assert row["snmp_type"] == resp.snmp_type
    assert "rate" not in row


def test_error_event(tmp_json):
    reporter, path = tmp_json

    try:
        raise ValueError("boom")
    except Exception as exc:
        reporter.error("hostX", exc)

    row = read_lines(path)[0]

    assert row["event"] == "error"
    assert row["host"] == "hostX"
    assert row["error"] == "boom"
    assert isinstance(row["timestamp"], float)


def test_multiple_events(tmp_json):
    reporter, path = tmp_json

    reporter.startup(1, 1.0)
    reporter.metric("h", DummyResponse(), 0.1)
    reporter.init_value("h", DummyResponse())
    reporter.error("h", Exception("x"))

    rows = read_lines(path)
    assert len(rows) == 4
