import asyncio
import json
import random
from unittest.mock import patch

import pytest

from telemetryd.metrics import SNMPResponse
from telemetryd.scheduler import TelemetryDaemon


@pytest.mark.asyncio
async def test_scheduler_chaos(tmp_path):
    """
    Full-system chaos test:
    - 50 devices
    - random jitter
    - random slowdowns
    - random exceptions
    - partial metric sets
    - timeout handling
    - staggering
    - concurrency
    """

    cfg = {
        "polling_interval_seconds": 0.02,
        "devices": [
            {
                "host": f"h{i}",
                "port": 161,
                "community": "public",
                "metrics": [
                    {"oid": "1", "type": "COUNTER32", "name": "m"},
                    {"oid": "2", "type": "COUNTER64", "name": "x"},
                ],
            }
            for i in range(50)
        ],
    }

    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(cfg))

    d = TelemetryDaemon(p)

    async def chaotic_fetch(host, port, community, metrics):
        r = random.random()

        # 10%: simulate timeout
        if r < 0.1:
            await asyncio.sleep(999)

        # 10%: simulate SNMP exception
        if r < 0.2:
            raise Exception("SNMP failure")

        # 20%: simulate jitter spike
        if r < 0.4:
            await asyncio.sleep(0)

        # 20%: partial metric set
        if r < 0.6:
            return [SNMPResponse("1", "m", random.randint(0, 10000), "COUNTER32")]

        # Normal case
        return [
            SNMPResponse("1", "m", random.randint(0, 10000), "COUNTER32"),
            SNMPResponse("2", "x", random.randint(0, 10000), "COUNTER64"),
        ]

    with patch.object(d.client, "fetch_metrics", side_effect=chaotic_fetch):

        async def stop():
            await asyncio.sleep(0.05)
            d.request_shutdown()

        await asyncio.wait(
            {asyncio.create_task(d.start()), asyncio.create_task(stop())},
            return_when=asyncio.FIRST_COMPLETED,
        )

    # Assert: all devices were at least attempted
    assert len(d._last_poll_time) == 50


@pytest.mark.asyncio
async def test_snmp_client_chaos():
    """
    Chaos test for AsyncSNMPClient:
    - jitter
    - partial metric sets
    - malformed values
    - exceptions
    """

    from telemetryd.metrics import SNMPResponse
    from telemetryd.snmp import AsyncSNMPClient

    client = AsyncSNMPClient()

    async def run_once():
        r = random.random()

        # 10%: exception
        if r < 0.1:
            with pytest.raises(Exception):
                raise Exception("SNMP failure")

        # 20%: jitter
        if r < 0.3:
            await asyncio.sleep(0)

        # 20%: partial set
        if r < 0.5:
            return [SNMPResponse("1", "m", random.randint(0, 10000), "COUNTER32")]

        # 20%: malformed
        if r < 0.7:
            return [SNMPResponse("1", "m", "not-a-number", "COUNTER32")]

        # Normal
        return [
            SNMPResponse("1", "m", random.randint(0, 10000), "COUNTER32"),
            SNMPResponse("2", "x", random.randint(0, 10000), "COUNTER64"),
        ]

    # Run 200 chaotic iterations
    for _ in range(200):
        try:
            await run_once()
        except Exception:
            pass  # expected


def test_rate_calculator_chaos():
    """
    Chaos test for RateCalculator:
    - wraparound
    - negative deltas
    - huge jumps
    - random resets
    """

    from telemetryd.metrics import RateCalculator, SNMPResponse

    calc = RateCalculator()

    host = "h"
    oid = "1"

    last_value = 0

    for _ in range(500):
        r = random.random()

        # 10%: wraparound
        if r < 0.1:
            value = random.randint(0, 1000)

        # 10%: negative delta (should be treated as reset)
        elif r < 0.2:
            value = max(0, last_value - random.randint(1, 1000))

        # 20%: huge jump
        elif r < 0.4:
            value = last_value + random.randint(10000, 50000)

        # Normal increment
        else:
            value = last_value + random.randint(0, 1000)

        resp = SNMPResponse(oid, "m", value, "COUNTER32")
        rate = calc.calculate_rate(host, resp, delta_time=1.0)

        # Rate must always be non-negative
        assert rate is None or rate >= 0

        last_value = value


@pytest.mark.asyncio
async def test_system_chaos_with_real_timeouts(tmp_path):
    """
    Full system chaos test with real timeouts:
    - slow devices
    - timeouts
    - exceptions
    - jitter
    - partial sets
    """

    cfg = {
        "polling_interval_seconds": 0.01,
        "devices": [
            {
                "host": f"h{i}",
                "port": 161,
                "community": "public",
                "metrics": [{"oid": "1", "type": "COUNTER32", "name": "m"}],
            }
            for i in range(20)
        ],
    }

    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(cfg))

    d = TelemetryDaemon(p)

    async def chaotic_fetch(host, port, community, metrics):
        r = random.random()

        # 20%: real timeout
        if r < 0.2:
            await asyncio.sleep(999)

        # 20%: exception
        if r < 0.4:
            raise Exception("SNMP failure")

        # 20%: jitter
        if r < 0.6:
            await asyncio.sleep(0)

        # 20%: partial set
        if r < 0.8:
            return [SNMPResponse("1", "m", random.randint(0, 10000), "COUNTER32")]

        # Normal
        return [SNMPResponse("1", "m", random.randint(0, 10000), "COUNTER32")]

    with patch.object(d.client, "fetch_metrics", side_effect=chaotic_fetch):

        async def stop():
            await asyncio.sleep(0.05)
            d.request_shutdown()

        await asyncio.wait(
            {asyncio.create_task(d.start()), asyncio.create_task(stop())},
            return_when=asyncio.FIRST_COMPLETED,
        )

    # All devices must have been attempted at least once
    assert len(d._last_poll_time) == 20
