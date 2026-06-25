import asyncio
import time

import pytest

from telemetryd.ext.prometheus_exporter import PrometheusTextExporter
from telemetryd.metrics import SNMPResponse


class DummyResponse(SNMPResponse):
    def __init__(self, oid="1.2.3", name="metric", value=42, snmp_type="gauge"):
        self.oid = oid
        self.name = name
        self.value = value
        self.snmp_type = snmp_type


@pytest.fixture
def exporter():
    return PrometheusTextExporter(
        host="127.0.0.1", port=0
    )  # port=0 → OS assigns free port


async def read_from_server(host, port):
    reader, writer = await asyncio.open_connection(host, port)
    data = await reader.read()  # read until EOF
    writer.close()
    return data.decode("utf-8")


def test_metric_storage(exporter):
    resp = DummyResponse(name="cpu", value=99)
    exporter.metric("host1", resp, rate=1.5)

    key = ("host1", "cpu")
    assert key in exporter._metrics

    value, rate, ts = exporter._metrics[key]
    assert value == 99
    assert rate == 1.5
    assert isinstance(ts, float)


def test_init_value_storage(exporter):
    resp = DummyResponse(name="mem", value=123)
    exporter.init_value("routerA", resp)

    key = ("routerA", "mem")
    assert key in exporter._metrics

    value, rate, ts = exporter._metrics[key]
    assert value == 123
    assert rate == 0.0
    assert isinstance(ts, float)


def test_error_noop(exporter):
    exporter.error("hostX", Exception("boom"))
    assert exporter._metrics == {}  # no metrics added


@pytest.mark.asyncio
async def test_prometheus_output_single_metric(exporter):
    resp = DummyResponse(name="temp", value=55)
    exporter.metric("sensor1", resp, rate=0.5)

    await exporter.start_server()
    host, port = exporter._server.sockets[0].getsockname()

    output = await read_from_server(host, port)

    assert 'telemetryd_temp_value{host="sensor1"} 55' in output
    assert 'telemetryd_temp_rate{host="sensor1"} 0.5' in output
    assert 'telemetryd_temp_timestamp{host="sensor1"}' in output


@pytest.mark.asyncio
async def test_prometheus_output_multiple_metrics(exporter):
    exporter.metric("h1", DummyResponse(name="cpu", value=10), 1.0)
    exporter.metric("h2", DummyResponse(name="mem", value=20), 2.0)

    await exporter.start_server()
    host, port = exporter._server.sockets[0].getsockname()

    output = await read_from_server(host, port)

    assert 'telemetryd_cpu_value{host="h1"} 10' in output
    assert 'telemetryd_mem_value{host="h2"} 20' in output


@pytest.mark.parametrize("value,rate", [(0, 0.0), (999, 3.14), (-5, 2.0)])
def test_metric_parametrized(exporter, value, rate):
    resp = DummyResponse(name="load", value=value)
    exporter.metric("hostA", resp, rate)

    stored = exporter._metrics[("hostA", "load")]
    assert stored[0] == value
    assert stored[1] == rate
    assert isinstance(stored[2], float)


def test_timestamp_increases(exporter):
    resp = DummyResponse(name="x", value=1)

    exporter.metric("h", resp, 1.0)
    ts1 = exporter._metrics[("h", "x")][2]

    time.sleep(0.01)

    exporter.metric("h", resp, 2.0)
    ts2 = exporter._metrics[("h", "x")][2]

    assert ts2 > ts1
