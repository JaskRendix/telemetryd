import asyncio
import json
from unittest.mock import Mock, patch

import pytest

from telemetryd.metrics import SNMPResponse
from telemetryd.scheduler import TelemetryDaemon, TelemetryReporter


@pytest.mark.asyncio
async def test_poll_device_initial(tmp_path):
    """
    - RateCalculator returns None on first observation
    - Daemon must call init_value() on first poll
    """
    cfg = {
        "polling_interval_seconds": 1,
        "devices": [
            {
                "host": "h",
                "port": 161,
                "community": "c",
                "metrics": [{"oid": "1", "type": "COUNTER32", "name": "m"}],
            }
        ],
    }
    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(cfg))

    reporter = TelemetryReporter()
    d = TelemetryDaemon(p, reporter=reporter)

    with (
        patch.object(
            d.client,
            "fetch_metrics",
            return_value=[SNMPResponse("1", "m", 100, "COUNTER32")],
        ),
        patch.object(reporter, "metric", new=Mock()) as mock_metric,
        patch.object(reporter, "init_value", new=Mock()) as mock_init,
    ):

        await d.poll_device(cfg["devices"][0])

    mock_metric.assert_not_called()
    mock_init.assert_called_once()


@pytest.mark.asyncio
async def test_poll_device_rate(tmp_path):
    """
    - First poll: init_value()
    - Second poll: metric() if calculator returns a rate
    """
    cfg = {
        "polling_interval_seconds": 1,
        "devices": [
            {
                "host": "h",
                "port": 161,
                "community": "c",
                "metrics": [{"oid": "1", "type": "COUNTER32", "name": "m"}],
            }
        ],
    }
    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(cfg))

    reporter = TelemetryReporter()
    d = TelemetryDaemon(p, reporter=reporter)

    seq = [
        [SNMPResponse("1", "m", 100, "COUNTER32")],
        [SNMPResponse("1", "m", 150, "COUNTER32")],
    ]

    async def fake_fetch(*args, **kwargs):
        return seq.pop(0)

    with (
        patch.object(d.client, "fetch_metrics", side_effect=fake_fetch),
        patch.object(
            d.calculator, "calculate_rate", side_effect=[None, 5.0]
        ) as mock_calc,
        patch.object(reporter, "metric", new=Mock()) as mock_metric,
        patch.object(reporter, "init_value", new=Mock()) as mock_init,
    ):

        await d.poll_device(cfg["devices"][0])  # first poll → init_value
        await asyncio.sleep(0.01)
        await d.poll_device(cfg["devices"][0])  # second poll → metric

    assert mock_calc.call_count == 2
    mock_init.assert_called_once()
    mock_metric.assert_called_once()

    host, resp, rate = mock_metric.call_args.args
    assert host == "h"
    assert resp.name == "m"
    assert rate == 5.0


@pytest.mark.asyncio
async def test_poll_device_error_handling(tmp_path):
    """
    Daemon must report errors via reporter.error()
    """
    cfg = {
        "polling_interval_seconds": 1,
        "devices": [
            {
                "host": "h",
                "port": 161,
                "community": "c",
                "metrics": [{"oid": "1", "type": "COUNTER32", "name": "m"}],
            }
        ],
    }
    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(cfg))

    reporter = TelemetryReporter()
    d = TelemetryDaemon(p, reporter=reporter)

    with (
        patch.object(d.client, "fetch_metrics", side_effect=Exception("x")),
        patch.object(reporter, "error", new=Mock()) as mock_error,
    ):

        await d.poll_device(cfg["devices"][0])

    mock_error.assert_called_once()
    host, exc = mock_error.call_args.args
    assert host == "h"
    assert isinstance(exc, Exception)


@pytest.mark.asyncio
async def test_stagger_uses_rng_per_device(tmp_path):
    """
    NEW contract:
    - Staggering happens inside _device_loop
    - RNG.uniform() must be called once per device
    """
    cfg = {
        "polling_interval_seconds": 1.0,
        "devices": [
            {"host": "h1", "port": 161, "community": "c", "metrics": []},
            {"host": "h2", "port": 161, "community": "c", "metrics": []},
            {"host": "h3", "port": 161, "community": "c", "metrics": []},
        ],
    }

    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(cfg))

    d = TelemetryDaemon(p)

    with patch.object(d.client._rng, "uniform", return_value=0.5) as mock_uniform:

        async def stop():
            await asyncio.sleep(0.05)
            d.request_shutdown()

        await asyncio.wait(
            {asyncio.create_task(d.start()), asyncio.create_task(stop())},
            return_when=asyncio.FIRST_COMPLETED,
        )

    assert mock_uniform.call_count == len(d.devices)


@pytest.mark.asyncio
async def test_start_shutdown(tmp_path):
    """
    Daemon must shut down cleanly when request_shutdown() is called
    """
    cfg = {
        "polling_interval_seconds": 0.01,
        "devices": [
            {
                "host": "h",
                "port": 161,
                "community": "c",
                "metrics": [{"oid": "1", "type": "COUNTER32", "name": "m"}],
            }
        ],
    }

    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(cfg))

    reporter = TelemetryReporter()
    d = TelemetryDaemon(p, reporter=reporter)

    async def fake_poll(*args, **kwargs):
        return None

    with patch.object(d, "poll_device", side_effect=fake_poll):

        async def stop():
            await asyncio.sleep(0.02)
            d.request_shutdown()
            return "stopped"

        done, pending = await asyncio.wait(
            {asyncio.create_task(d.start()), asyncio.create_task(stop())},
            return_when=asyncio.FIRST_COMPLETED,
        )

        for t in pending:
            t.cancel()

        assert any(t.result() == "stopped" for t in done)
