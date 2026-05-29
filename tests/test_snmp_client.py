import random

import pytest

from telemetryd.metrics import SNMPResponse
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
