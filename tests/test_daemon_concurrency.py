import asyncio
import json
import time
from unittest.mock import patch

import pytest

from telemetryd.metrics import SNMPResponse
from telemetryd.scheduler import TelemetryDaemon


@pytest.mark.asyncio
async def test_concurrency_stress(tmp_path):
    cfg = {
        "polling_interval_seconds": 0.01,
        "devices": [
            {
                "host": f"h{i}",
                "port": 161,
                "community": "c",
                "metrics": [{"oid": "1", "type": "COUNTER32", "name": "m"}],
            }
            for i in range(50)
        ],
    }

    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(cfg))
    d = TelemetryDaemon(p)

    # Track poll attempts instead of relying on _last_poll_time
    poll_counts = {dev["host"]: 0 for dev in d.devices}

    original_poll = d.poll_device

    async def wrapped_poll(device):
        poll_counts[device["host"]] += 1
        return await original_poll(device)

    d.poll_device = wrapped_poll

    async def fake_fetch(host, port, community, metrics):
        await asyncio.sleep(0)
        return [SNMPResponse("1", "m", 100, "COUNTER32")]

    with patch.object(d.client, "fetch_metrics", side_effect=fake_fetch):

        async def run_once():
            await d.run_once(cfg["devices"])

        t0 = time.time()
        await asyncio.gather(*(run_once() for _ in range(20)))
        t1 = time.time()

        # concurrency timing check
        assert t1 - t0 < 1.5

        assert all(count > 0 for count in poll_counts.values())
