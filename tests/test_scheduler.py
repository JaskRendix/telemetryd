import asyncio
import json
from unittest.mock import AsyncMock, Mock

import pytest

from telemetryd.metrics import SNMPResponse
from telemetryd.scheduler import TelemetryDaemon, TelemetryReporter


class DummyResponse(SNMPResponse):
    def __init__(self, name="metric", value=10):
        self.name = name
        self.value = value
        self.oid = "1.2.3"
        self.snmp_type = "gauge"


@pytest.fixture
def config_file(tmp_path):
    cfg = {
        "polling_interval_seconds": 0.1,
        "devices": [
            {
                "host": "dev1",
                "port": 161,
                "community": "public",
                "metrics": ["m1", "m2"],
            }
        ],
    }
    path = tmp_path / "config.json"
    path.write_text(json.dumps(cfg))
    return path


@pytest.fixture
def mock_client():
    client = Mock()
    client.fetch_metrics = AsyncMock()
    client._rng = Mock()
    client._rng.uniform = Mock(return_value=0.0)
    return client


@pytest.fixture
def mock_calculator():
    calc = Mock()
    calc.calculate_rate = Mock(return_value=1.23)
    return calc


@pytest.fixture
def mock_reporter():
    rep = Mock(spec=TelemetryReporter)
    return rep


def test_load_config(config_file, mock_client, mock_calculator, mock_reporter):
    d = TelemetryDaemon(config_file, mock_client, mock_calculator, mock_reporter)
    assert d.interval == 0.1
    assert len(d.devices) == 1
    assert d.devices[0]["host"] == "dev1"


@pytest.mark.asyncio
async def test_poll_device_success(
    config_file, mock_client, mock_calculator, mock_reporter
):
    mock_client.fetch_metrics.return_value = [
        DummyResponse("cpu", 50),
        DummyResponse("mem", 100),
    ]

    d = TelemetryDaemon(config_file, mock_client, mock_calculator, mock_reporter)

    await d.poll_device(d.devices[0])

    assert mock_client.fetch_metrics.called
    assert mock_calculator.calculate_rate.call_count == 2
    assert mock_reporter.metric.call_count == 2


@pytest.mark.asyncio
async def test_poll_device_init_value(config_file, mock_client, mock_reporter):
    # RateCalculator returns None → init_value path
    calc = Mock()
    calc.calculate_rate = Mock(return_value=None)

    mock_client.fetch_metrics.return_value = [DummyResponse("cpu", 10)]

    d = TelemetryDaemon(config_file, mock_client, calc, mock_reporter)

    await d.poll_device(d.devices[0])

    mock_reporter.init_value.assert_called_once()


@pytest.mark.asyncio
async def test_poll_device_error(
    config_file, mock_client, mock_calculator, mock_reporter
):
    mock_client.fetch_metrics.side_effect = RuntimeError("boom")

    d = TelemetryDaemon(config_file, mock_client, mock_calculator, mock_reporter)

    await d.poll_device(d.devices[0])

    mock_reporter.error.assert_called_once()
    assert "boom" in str(mock_reporter.error.call_args[0][1])


@pytest.mark.asyncio
async def test_run_once(config_file, mock_client, mock_calculator, mock_reporter):
    mock_client.fetch_metrics.return_value = [DummyResponse("cpu", 1)]

    d = TelemetryDaemon(config_file, mock_client, mock_calculator, mock_reporter)

    duration = await d.run_once()

    assert duration >= 0
    assert mock_client.fetch_metrics.called


@pytest.mark.asyncio
async def test_start_stagger_and_shutdown(
    config_file, mock_client, mock_calculator, mock_reporter
):
    mock_client.fetch_metrics.return_value = [DummyResponse("cpu", 1)]

    d = TelemetryDaemon(config_file, mock_client, mock_calculator, mock_reporter)

    # Trigger shutdown after first loop iteration
    async def trigger_shutdown():
        await asyncio.sleep(0.05)
        d.request_shutdown()

    shutdown_task = asyncio.create_task(trigger_shutdown())

    await asyncio.wait_for(d.start(), timeout=1.0)

    shutdown_task.cancel()

    # startup() must be called
    mock_reporter.startup.assert_called_once()

    # At least one metric call must have happened
    assert mock_reporter.metric.called or mock_reporter.init_value.called


def test_request_shutdown(config_file, mock_client, mock_calculator, mock_reporter):
    d = TelemetryDaemon(config_file, mock_client, mock_calculator, mock_reporter)
    assert not d._shutdown_event.is_set()
    d.request_shutdown()
    assert d._shutdown_event.is_set()
