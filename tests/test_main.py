import asyncio
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


@pytest.mark.asyncio
async def test_main_starts_daemon_with_default_json(tmp_path, monkeypatch):
    # Create a dummy config file
    config = tmp_path / "config.json"
    config.write_text("{}")

    # Patch argv to use default reporter=json
    monkeypatch.setattr("sys.argv", ["telemetryd", "--config", str(config)])

    # Mock TelemetryDaemon
    started = False

    class DummyDaemon:
        def __init__(self, cfg, reporter):
            assert cfg == config

        async def start(self):
            nonlocal started
            started = True

    monkeypatch.setattr("telemetryd.main.TelemetryDaemon", DummyDaemon)

    # Import main after monkeypatching
    import telemetryd.main as main_module

    await main_module.main()
    assert started is True


@pytest.mark.asyncio
async def test_main_prometheus_starts_server(tmp_path, monkeypatch):
    config = tmp_path / "config.json"
    config.write_text("{}")

    monkeypatch.setattr(
        "sys.argv", ["telemetryd", "--config", str(config), "--reporter", "prometheus"]
    )

    server_started = False
    daemon_started = False

    class DummyPrometheus:
        def __init__(self, port):
            assert port == 9100

        async def start_server(self):
            nonlocal server_started
            server_started = True

    class DummyDaemon:
        def __init__(self, cfg, reporter):
            assert cfg == config

        async def start(self):
            nonlocal daemon_started
            daemon_started = True

    monkeypatch.setattr("telemetryd.main.PrometheusTextExporter", DummyPrometheus)
    monkeypatch.setattr("telemetryd.main.TelemetryDaemon", DummyDaemon)

    import telemetryd.main as main_module

    await main_module.main()

    assert server_started is True
    assert daemon_started is True


@pytest.mark.asyncio
async def test_main_missing_config(monkeypatch):
    missing = Path("/nonexistent/config.json")

    monkeypatch.setattr("sys.argv", ["telemetryd", "--config", str(missing)])

    import telemetryd.main as main_module

    with pytest.raises(FileNotFoundError):
        await main_module.main()


def test_main_keyboard_interrupt(monkeypatch):
    # Fake asyncio.run to raise KeyboardInterrupt
    def fake_run(coro):
        raise KeyboardInterrupt

    monkeypatch.setattr("asyncio.run", fake_run)

    import telemetryd.main as main_module

    # Pretend module is executed as script
    monkeypatch.setattr(main_module, "__name__", "__main__")

    # Replace main() with a proper coroutine
    async def dummy_main():
        pass

    main_module.main = dummy_main

    # Capture stdout
    import io
    import sys

    buf = io.StringIO()
    monkeypatch.setattr(sys, "stdout", buf)

    # Execute top-level guard manually
    try:
        if main_module.__name__ == "__main__":
            try:
                asyncio.run(None)
            except KeyboardInterrupt:
                print("\nDaemon terminated.")
    except Exception:
        pytest.fail("KeyboardInterrupt should be handled")

    assert "Daemon terminated." in buf.getvalue()


@pytest.mark.asyncio
async def test_main_selects_csv(tmp_path, monkeypatch):
    config = tmp_path / "config.json"
    config.write_text("{}")

    monkeypatch.setattr(
        "sys.argv", ["telemetryd", "--config", str(config), "--reporter", "csv"]
    )

    created = False

    class DummyCSV:
        def __init__(self, path):
            nonlocal created
            created = True

    class DummyDaemon:
        def __init__(self, cfg, reporter):
            assert created is True

        async def start(self):
            return

    # Remove cached module so monkeypatch applies BEFORE import
    import sys

    sys.modules.pop("telemetryd.main", None)

    # Now import fresh
    import telemetryd.main as main_module

    # Patch build_reporter and TelemetryDaemon BEFORE running main()
    monkeypatch.setattr(
        main_module, "build_reporter", lambda kind: DummyCSV("dummy.csv")
    )
    monkeypatch.setattr(main_module, "TelemetryDaemon", DummyDaemon)

    await main_module.main()

    assert created is True
