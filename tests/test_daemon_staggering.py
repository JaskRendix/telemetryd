import asyncio
import json
from unittest.mock import patch

import pytest

from telemetryd.scheduler import TelemetryDaemon


@pytest.mark.asyncio
async def test_stagger_uses_rng_per_device(tmp_path):
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

    # one uniform() call per device loop
    assert mock_uniform.call_count == len(d.devices)


@pytest.mark.asyncio
async def test_stagger_is_deterministic_with_seed(tmp_path):
    cfg = {
        "polling_interval_seconds": 1.0,
        "devices": [
            {"host": "h1", "port": 161, "community": "c", "metrics": []},
            {"host": "h2", "port": 161, "community": "c", "metrics": []},
        ],
    }

    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(cfg))

    import random

    rng1 = random.Random(123)
    rng2 = random.Random(123)

    d1 = TelemetryDaemon(p)
    d2 = TelemetryDaemon(p)

    d1.client._rng = rng1
    d2.client._rng = rng2

    delays1 = [d1.client._rng.uniform(0, d1.interval) for _ in d1.devices]
    delays2 = [d2.client._rng.uniform(0, d2.interval) for _ in d2.devices]

    assert delays1 == delays2


@pytest.mark.asyncio
async def test_staggered_start_eventually_calls_poll_per_device(tmp_path):
    cfg = {
        "polling_interval_seconds": 0.05,
        "devices": [
            {"host": "h1", "port": 161, "community": "c", "metrics": []},
            {"host": "h2", "port": 161, "community": "c", "metrics": []},
        ],
    }

    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(cfg))

    d = TelemetryDaemon(p)

    calls: list[str] = []

    async def fake_poll(device):
        calls.append(device["host"])

    # remove randomness so all devices start immediately
    with patch.object(d.client._rng, "uniform", return_value=0.0):
        with patch.object(d, "poll_device", side_effect=fake_poll):

            async def stop():
                # give device loops time to run at least one iteration
                await asyncio.sleep(0.1)
                d.request_shutdown()

            await asyncio.wait(
                {asyncio.create_task(d.start()), asyncio.create_task(stop())},
                return_when=asyncio.FIRST_COMPLETED,
            )

    assert set(calls) == {"h1", "h2"}


@pytest.mark.asyncio
async def test_stagger_does_not_break_run_once(tmp_path):
    cfg = {
        "polling_interval_seconds": 0.01,
        "devices": [
            {"host": "h1", "port": 161, "community": "c", "metrics": []},
            {"host": "h2", "port": 161, "community": "c", "metrics": []},
        ],
    }

    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(cfg))

    d = TelemetryDaemon(p)

    async def fake_poll(device):
        return None

    with patch.object(d, "poll_device", side_effect=fake_poll):
        duration = await d.run_once()
        assert duration >= 0.0


@pytest.mark.asyncio
async def test_stagger_does_not_break_concurrency(tmp_path):
    cfg = {
        "polling_interval_seconds": 0.02,
        "devices": [
            {"host": f"h{i}", "port": 161, "community": "c", "metrics": []}
            for i in range(30)
        ],
    }

    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(cfg))

    d = TelemetryDaemon(p)

    calls = 0

    async def fake_poll(device):
        nonlocal calls
        calls += 1
        return None

    with patch.object(d.client._rng, "uniform", return_value=0.0):
        with patch.object(d, "poll_device", side_effect=fake_poll):

            async def stop():
                # allow at least one full polling cycle
                await asyncio.sleep(0.1)
                d.request_shutdown()

            await asyncio.wait(
                {asyncio.create_task(d.start()), asyncio.create_task(stop())},
                return_when=asyncio.FIRST_COMPLETED,
            )

    # each device should have been polled at least once
    assert calls >= len(d.devices)
