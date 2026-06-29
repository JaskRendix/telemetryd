import asyncio
import logging

log = logging.getLogger(__name__)


class HealthServer:
    """
    Minimal health endpoint.
    Exposes:
      - /health → always 200 OK
      - /ready  → 200 OK only after first poll
    """

    def __init__(self, host="0.0.0.0", port=8081):
        self.host = host
        self.port = port
        self._ready = False
        self._server = None

    def mark_ready(self):
        """Called by scheduler after first successful poll."""
        self._ready = True

    async def _handle(self, reader, writer):
        try:
            data = await reader.readline()
            request = data.decode("ascii", errors="ignore")

            if request.startswith("GET /health"):
                response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"
            elif request.startswith("GET /ready"):
                if self._ready:
                    response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"
                else:
                    response = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 3\r\n\r\nNOK"
            else:
                response = (
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\nNOT FOUND"
                )

            writer.write(response.encode("ascii"))
            await writer.drain()
        except Exception as e:
            log.warning(f"health endpoint error: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def start(self):
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        log.info(f"health endpoint listening on {self.host}:{self.port}")

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            log.info("health endpoint stopped")
