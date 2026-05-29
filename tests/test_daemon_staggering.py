import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest

from telemetryd.scheduler import TelemetryDaemon


@pytest.mark.asyncio
async def test_stagger_offsets_are_created(tmp_path):
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

    assert len(d._stagger) == 3
    assert all(0 <= v <= d.interval for v in d._stagger.values())


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

    rng = random.Random(123)

    d1 = TelemetryDaemon(p, client=None)
    d1.client._rng = rng
    d1._stagger = {
        d["host"]: d1.client._rng.uniform(0, d1.interval) for d in d1.devices
    }

    rng2 = random.Random(123)
    d2 = TelemetryDaemon(p, client=None)
    d2.client._rng = rng2
    d2._stagger = {
        d["host"]: d2.client._rng.uniform(0, d2.interval) for d in d2.devices
    }

    assert d1._stagger == d2._stagger


@pytest.mark.asyncio
async def test_staggered_start_calls_poll_in_order(tmp_path):
    cfg = {
        "polling_interval_seconds": 1.0,
        "devices": [
            {"host": "h1", "port": 161, "community": "c", "metrics": []},
            {"host": "h2", "port": 161, "community": "c", "metrics": []},
        ],
    }

    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(cfg))

    d = TelemetryDaemon(p)

    calls = []

    async def fake_poll(device):
        calls.append(device["host"])

    with patch.object(d, "poll_device", side_effect=fake_poll):
        # Patch sleep so staggering is instant but still recorded
        with patch("asyncio.sleep", new=AsyncMock()):

            async def stop():
                await asyncio.sleep(0)
                d.request_shutdown()

            await asyncio.wait(
                {asyncio.create_task(d.start()), asyncio.create_task(stop())},
                return_when=asyncio.FIRST_COMPLETED,
            )

    # Both devices must have been polled once during staggering
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
        "polling_interval_seconds": 0.01,
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
        await asyncio.sleep(0)
        return None

    with patch.object(d, "poll_device", side_effect=fake_poll):
        with patch("asyncio.sleep", new=AsyncMock()):

            async def stop():
                await asyncio.sleep(0)
                d.request_shutdown()

            await asyncio.wait(
                {asyncio.create_task(d.start()), asyncio.create_task(stop())},
                return_when=asyncio.FIRST_COMPLETED,
            )

    # All devices should have been polled at least once during staggering
    assert calls >= 30


@pytest.mark.asyncio
async def test_stagger_does_not_break_concurrency(tmp_path):
    cfg = {
        "polling_interval_seconds": 0.01,
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
        await asyncio.sleep(0)
        return None

    with patch.object(d, "poll_device", side_effect=fake_poll):
        with patch("asyncio.sleep", new=AsyncMock()):

            async def stop():
                await asyncio.sleep(0)
                d.request_shutdown()

            await asyncio.wait(
                {asyncio.create_task(d.start()), asyncio.create_task(stop())},
                return_when=asyncio.FIRST_COMPLETED,
            )

    assert calls >= 30
