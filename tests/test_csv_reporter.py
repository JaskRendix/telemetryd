import csv
from pathlib import Path

import pytest

from telemetryd.ext.csv_reporter import CSVReporter
from telemetryd.metrics import SNMPResponse


class DummyResponse(SNMPResponse):
    """A minimal stand‑in for SNMPResponse if needed."""

    def __init__(self, oid="1.2.3", name="test_metric", value=42, snmp_type="gauge"):
        self.oid = oid
        self.name = name
        self.value = value
        self.snmp_type = snmp_type


@pytest.fixture
def tmp_csv(tmp_path):
    """Create a temporary CSVReporter pointing to a fresh file."""
    path = tmp_path / "out" / "metrics.csv"
    reporter = CSVReporter(path)
    yield reporter, path
    reporter.close()


def read_csv(path: Path):
    with path.open() as fh:
        return list(csv.reader(fh))


def test_header_written_once(tmp_csv):
    reporter, path = tmp_csv

    rows = read_csv(path)
    assert len(rows) == 1
    assert rows[0] == [
        "timestamp",
        "event",
        "host",
        "oid",
        "name",
        "value",
        "rate",
        "snmp_type",
        "error",
    ]


def test_header_not_rewritten_on_append(tmp_path):
    path = tmp_path / "metrics.csv"

    # First instance writes header
    r1 = CSVReporter(path)
    r1.startup(5, 1.0)
    r1.close()

    # Second instance should NOT write header again
    r2 = CSVReporter(path)
    r2.startup(10, 2.0)
    r2.close()

    rows = read_csv(path)
    assert len(rows) == 3  # header + 2 startup rows


def test_startup_row(tmp_csv):
    reporter, path = tmp_csv

    reporter.startup(device_count=3, interval=5.0)
    rows = read_csv(path)

    assert rows[1][1] == "startup"
    assert rows[1][5] == "3"
    assert rows[1][6] == "5.0"
    assert rows[1][2] == ""  # host empty
    assert rows[1][8] == ""  # error empty
    assert float(rows[1][0]) > 0  # timestamp


@pytest.mark.parametrize(
    "value,rate",
    [
        (123, 1.5),
        ("abc", 0.0),
        (None, 9.9),
    ],
)
def test_metric_rows(tmp_csv, value, rate):
    reporter, path = tmp_csv

    resp = DummyResponse(value=value)
    reporter.metric("host1", resp, rate)

    rows = read_csv(path)
    row = rows[1]

    assert row[1] == "metric"
    assert row[2] == "host1"
    assert row[3] == resp.oid
    assert row[4] == resp.name

    expected_value = "" if value is None else str(value)
    assert row[5] == expected_value

    assert row[6] == str(rate)
    assert row[7] == resp.snmp_type
    assert row[8] == ""


def test_init_value(tmp_csv):
    reporter, path = tmp_csv

    resp = DummyResponse(value=999)
    reporter.init_value("routerA", resp)

    rows = read_csv(path)
    row = rows[1]

    assert row[1] == "init"
    assert row[2] == "routerA"
    assert row[5] == "999"
    assert row[6] == ""  # rate empty


def test_error_row(tmp_csv):
    reporter, path = tmp_csv

    try:
        raise ValueError("boom")
    except Exception as exc:
        reporter.error("hostX", exc)

    rows = read_csv(path)
    row = rows[1]

    assert row[1] == "error"
    assert row[2] == "hostX"
    assert row[8] == "boom"
    assert row[3] == ""  # oid empty
    assert row[5] == ""  # value empty


def test_directory_created(tmp_path):
    path = tmp_path / "nested" / "deep" / "metrics.csv"
    reporter = CSVReporter(path)
    reporter.close()

    assert path.exists()


def test_multiple_writes_flush(tmp_csv):
    reporter, path = tmp_csv

    reporter.startup(1, 1.0)
    reporter.metric("h", DummyResponse(), 0.1)
    reporter.init_value("h", DummyResponse())
    reporter.error("h", Exception("x"))

    rows = read_csv(path)
    assert len(rows) == 5  # header + 4 events


def test_timestamp_is_float(tmp_csv):
    reporter, path = tmp_csv

    reporter.startup(1, 1.0)
    ts = float(read_csv(path)[1][0])
    assert isinstance(ts, float)
    assert ts > 0
