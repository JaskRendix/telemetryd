import asyncio

import pytest

from telemetryd.ext.health import HealthServer


@pytest.mark.asyncio
async def test_health_endpoint_basic_health():
    server = HealthServer(host="127.0.0.1", port=0)  # ephemeral port
    await server.start()

    host, port = server._server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(host, port)
    writer.write(b"GET /health HTTP/1.1\r\n\r\n")
    await writer.drain()

    data = await reader.read(1024)
    assert b"200 OK" in data
    assert b"OK" in data

    writer.close()
    await writer.wait_closed()
    await server.stop()


@pytest.mark.asyncio
async def test_health_endpoint_ready_false():
    server = HealthServer(host="127.0.0.1", port=0)
    await server.start()

    host, port = server._server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(host, port)
    writer.write(b"GET /ready HTTP/1.1\r\n\r\n")
    await writer.drain()

    data = await reader.read(1024)
    assert b"503 Service Unavailable" in data
    assert b"NOK" in data

    writer.close()
    await writer.wait_closed()
    await server.stop()


@pytest.mark.asyncio
async def test_health_endpoint_ready_true():
    server = HealthServer(host="127.0.0.1", port=0)
    server.mark_ready()
    await server.start()

    host, port = server._server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(host, port)
    writer.write(b"GET /ready HTTP/1.1\r\n\r\n")
    await writer.drain()

    data = await reader.read(1024)
    assert b"200 OK" in data
    assert b"OK" in data

    writer.close()
    await writer.wait_closed()
    await server.stop()


@pytest.mark.asyncio
async def test_health_endpoint_404():
    server = HealthServer(host="127.0.0.1", port=0)
    await server.start()

    host, port = server._server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(host, port)
    writer.write(b"GET /unknown HTTP/1.1\r\n\r\n")
    await writer.drain()

    data = await reader.read(1024)
    assert b"404 Not Found" in data
    assert b"NOT FOUND" in data

    writer.close()
    await writer.wait_closed()
    await server.stop()


@pytest.mark.asyncio
async def test_health_server_stop():
    server = HealthServer(host="127.0.0.1", port=0)
    await server.start()

    # Capture host/port BEFORE stopping the server
    host, port = server._server.sockets[0].getsockname()

    await server.stop()

    # After stop, server should not accept connections
    with pytest.raises(ConnectionRefusedError):
        await asyncio.open_connection(host, port)
