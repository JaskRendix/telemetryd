import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest

from telemetryd.metrics import SNMPResponse
from telemetryd.scheduler import TelemetryDaemon, TelemetryReporter


@pytest.mark.asyncio
async def test_load_config(tmp_path):
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
    d = TelemetryDaemon(p)
    assert d.devices[0]["host"] == "h"


@pytest.mark.asyncio
async def test_poll_device_initial(tmp_path):
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

    with patch.object(
        d.client,
        "fetch_metrics",
        return_value=[SNMPResponse("1", "m", 100, "COUNTER32")],
    ):
        await d.poll_device(cfg["devices"][0])
        assert "h" in d._last_poll_time


@pytest.mark.asyncio
async def test_poll_device_rate(tmp_path):
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

    with patch.object(d.client, "fetch_metrics", side_effect=fake_fetch):
        await d.poll_device(cfg["devices"][0])
        t0 = d._last_poll_time["h"]
        await asyncio.sleep(0.01)
        await d.poll_device(cfg["devices"][0])
        assert d.calculator._history[("h", "1")] == 150
        assert d._last_poll_time["h"] != t0


@pytest.mark.asyncio
async def test_poll_device_error_handling(tmp_path):
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

    with patch.object(d.client, "fetch_metrics", side_effect=Exception("x")):
        await d.poll_device(cfg["devices"][0])
        assert "h" in d._last_poll_time


@pytest.mark.asyncio
async def test_run_once(tmp_path):
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
        duration = await d.run_once()
        assert duration >= 0.0


@pytest.mark.asyncio
async def test_start_shutdown(tmp_path):
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

        with patch("asyncio.sleep", new=AsyncMock()):
            done, pending = await asyncio.wait(
                {asyncio.create_task(d.start()), asyncio.create_task(stop())},
                return_when=asyncio.FIRST_COMPLETED,
            )

            for t in pending:
                t.cancel()

            assert any(t.result() == "stopped" for t in done)
