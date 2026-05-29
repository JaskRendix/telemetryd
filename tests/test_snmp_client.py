import asyncio
import json
import random
from unittest.mock import AsyncMock, patch

import pytest

from telemetryd.metrics import SNMPResponse
from telemetryd.scheduler import TelemetryDaemon, TelemetryReporter
from telemetryd.snmp import AsyncSNMPClient


@pytest.mark.asyncio
async def test_initial_accumulator_creation():
    client = AsyncSNMPClient()
    metrics = [{"oid": "1", "type": "COUNTER32", "name": "m"}]
    res = await client.fetch_metrics("h", 161, "c", metrics)
    assert isinstance(res[0], SNMPResponse)
    assert client._mock_accumulators["h:1"] >= 1000


@pytest.mark.asyncio
async def test_monotonic_increment():
    client = AsyncSNMPClient()
    metrics = [{"oid": "1", "type": "COUNTER32", "name": "m"}]
    r1 = await client.fetch_metrics("h", 161, "c", metrics)
    v1 = r1[0].value
    r2 = await client.fetch_metrics("h", 161, "c", metrics)
    v2 = r2[0].value
    assert v2 != v1


@pytest.mark.asyncio
async def test_counter32_wraparound():
    client = AsyncSNMPClient()
    key = "h:1"
    client._mock_accumulators[key] = 2**32 - 5
    metrics = [{"oid": "1", "type": "COUNTER32", "name": "m"}]
    r = await client.fetch_metrics("h", 161, "c", metrics)
    assert r[0].value < 50


@pytest.mark.asyncio
async def test_counter64_wraparound():
    client = AsyncSNMPClient()
    key = "h:1"
    client._mock_accumulators[key] = 2**64 - 5
    metrics = [{"oid": "1", "type": "COUNTER64", "name": "m"}]
    r = await client.fetch_metrics("h", 161, "c", metrics)
    assert r[0].value < 50


@pytest.mark.asyncio
async def test_independent_hosts():
    client = AsyncSNMPClient()
    metrics = [{"oid": "1", "type": "COUNTER32", "name": "m"}]
    r1 = await client.fetch_metrics("h1", 161, "c", metrics)
    r2 = await client.fetch_metrics("h2", 161, "c", metrics)
    assert r1[0].value != r2[0].value


@pytest.mark.asyncio
async def test_independent_oids():
    client = AsyncSNMPClient()
    metrics = [
        {"oid": "1", "type": "COUNTER32", "name": "m1"},
        {"oid": "2", "type": "COUNTER32", "name": "m2"},
    ]
    r = await client.fetch_metrics("h", 161, "c", metrics)
    assert r[0].oid != r[1].oid
    assert r[0].value != r[1].value


@pytest.mark.asyncio
async def test_forced_overflow_branch():
    rng = random.Random()
    rng.random = lambda: 0.99
    rng.randint = lambda a, b: 50

    client = AsyncSNMPClient(rng=rng)
    metrics = [{"oid": "1", "type": "COUNTER32", "name": "m"}]

    client._mock_accumulators["h:1"] = 100

    r = await client.fetch_metrics("h", 161, "c", metrics)
    v = r[0].value

    assert v < 100


@pytest.mark.asyncio
async def test_failure_timeout():
    rng = random.Random()
    rng.random = lambda: 0.0  # always trigger failure
    client = AsyncSNMPClient(rng=rng, failure_rate=1.0)

    metrics = [{"oid": "1", "type": "COUNTER32", "name": "m"}]

    with pytest.raises(TimeoutError):
        await client.fetch_metrics("h", 161, "c", metrics)


@pytest.mark.asyncio
async def test_malformed_response():
    rng = random.Random()
    rng.random = lambda: 0.0  # always trigger malformed
    client = AsyncSNMPClient(rng=rng, malformed_rate=1.0)

    metrics = [{"oid": "1", "type": "COUNTER32", "name": "m"}]

    with pytest.raises(ValueError):
        await client.fetch_metrics("h", 161, "c", metrics)


@pytest.mark.asyncio
async def test_partial_metric_set():
    rng = random.Random()
    rng.random = lambda: 0.0  # always trigger partial set
    client = AsyncSNMPClient(rng=rng, partial_rate=1.0)

    metrics = [
        {"oid": "1", "type": "COUNTER32", "name": "m1"},
        {"oid": "2", "type": "COUNTER32", "name": "m2"},
        {"oid": "3", "type": "COUNTER32", "name": "m3"},
    ]

    r = await client.fetch_metrics("h", 161, "c", metrics)
    assert 1 <= len(r) <= 2


@pytest.mark.asyncio
async def test_jitter_spike_delay():
    calls = []

    async def fake_sleep(duration):
        calls.append(duration)

    rng = random.Random()
    rng.random = lambda: 0.0  # always trigger jitter
    rng.uniform = lambda a, b: 0.123  # deterministic jitter

    client = AsyncSNMPClient(
        rng=rng,
        jitter_rate=1.0,
        jitter_range=(0.2, 0.5),
        latency_range=(0.01, 0.01),
    )

    metrics = [{"oid": "1", "type": "COUNTER32", "name": "m"}]

    with patch("asyncio.sleep", side_effect=fake_sleep):
        await client.fetch_metrics("h", 161, "c", metrics)

    assert len(calls) == 2
    assert calls[1] == 0.123


@pytest.mark.asyncio
async def test_rng_injection_deterministic():
    rng = random.Random(123)
    client = AsyncSNMPClient(rng=rng)

    metrics = [{"oid": "1", "type": "COUNTER32", "name": "m"}]

    r1 = await client.fetch_metrics("h", 161, "c", metrics)
    r2 = await client.fetch_metrics("h", 161, "c", metrics)

    assert r1[0].value != r2[0].value
    assert client._mock_accumulators["h:1"] == r2[0].value


@pytest.mark.asyncio
async def test_stress_jitter_and_partial_sets():
    rng = random.Random(123)

    client = AsyncSNMPClient(
        rng=rng,
        latency_range=(0.001, 0.005),
        jitter_rate=0.5,
        jitter_range=(0.01, 0.02),
        partial_rate=0.5,
    )

    metrics = [
        {"oid": "1", "type": "COUNTER32", "name": "m1"},
        {"oid": "2", "type": "COUNTER32", "name": "m2"},
        {"oid": "3", "type": "COUNTER32", "name": "m3"},
        {"oid": "4", "type": "COUNTER32", "name": "m4"},
    ]

    async def one_cycle():
        r = await client.fetch_metrics("h", 161, "c", metrics)
        assert 1 <= len(r) <= 4
        for resp in r:
            assert isinstance(resp, SNMPResponse)

    tasks = [one_cycle() for _ in range(200)]
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_scheduler_with_snmp_failures(tmp_path):
    cfg = {
        "polling_interval_seconds": 0.01,
        "devices": [
            {
                "host": "ok",
                "port": 161,
                "community": "c",
                "metrics": [{"oid": "1", "type": "COUNTER32", "name": "m"}],
            },
            {
                "host": "fail",
                "port": 161,
                "community": "c",
                "metrics": [{"oid": "1", "type": "COUNTER32", "name": "m"}],
            },
        ],
    }

    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(cfg))

    reporter = TelemetryReporter()
    daemon = TelemetryDaemon(p, reporter=reporter)

    async def fake_fetch(host, port, community, metrics):
        if host == "fail":
            raise TimeoutError("simulated failure")
        return [SNMPResponse("1", "m", 100, "COUNTER32")]

    with patch.object(daemon.client, "fetch_metrics", side_effect=fake_fetch):
        with patch("asyncio.sleep", new=AsyncMock()):

            async def stop():
                await asyncio.sleep(0)
                daemon.request_shutdown()
                return "stopped"

            done, pending = await asyncio.wait(
                {
                    asyncio.create_task(daemon.start()),
                    asyncio.create_task(stop()),
                },
                return_when=asyncio.FIRST_COMPLETED,
            )

            for t in pending:
                t.cancel()

            assert any(t.result() == "stopped" for t in done)

    assert "ok" in daemon._last_poll_time
    assert "fail" in daemon._last_poll_time
