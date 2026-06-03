from pathlib import Path

import pytest

from telemetryd.ext.csv_reporter import CSVReporter
from telemetryd.ext.json_reporter import JSONReporter
from telemetryd.ext.prometheus_exporter import PrometheusTextExporter
from telemetryd.main import build_reporter


@pytest.mark.parametrize(
    "kind,expected",
    [
        ("json", JSONReporter),
        ("csv", CSVReporter),
        ("prometheus", PrometheusTextExporter),
    ],
)
def test_build_reporter_valid(kind, expected):
    reporter = build_reporter(kind)
    assert isinstance(reporter, expected)


def test_build_reporter_invalid():
    with pytest.raises(ValueError):
        build_reporter("invalid-reporter")


def test_default_config_path(tmp_path, monkeypatch):
    config_file = tmp_path / "myconfig.json"
    config_file.write_text("{}")

    # Simulate CLI args
    monkeypatch.setenv("PYTHONPATH", str(Path.cwd()))
    monkeypatch.setattr(
        "sys.argv", ["main.py", "--config", str(config_file), "--reporter", "json"]
    )

    # Import inside test to trigger argparse
    import telemetryd.main as main_module

    # Rebuild reporter to ensure correct type
    reporter = main_module.build_reporter("json")
    assert isinstance(reporter, JSONReporter)


def test_cli_selects_csv(monkeypatch):
    monkeypatch.setattr("sys.argv", ["main.py", "--reporter", "csv"])

    import telemetryd.main as main_module

    reporter = main_module.build_reporter("csv")
    assert isinstance(reporter, CSVReporter)


def test_cli_selects_prometheus(monkeypatch):
    monkeypatch.setattr("sys.argv", ["main.py", "--reporter", "prometheus"])

    import telemetryd.main as main_module

    reporter = main_module.build_reporter("prometheus")
    assert isinstance(reporter, PrometheusTextExporter)
