import asyncio
import time

from telemetryd.metrics import SNMPResponse


class PrometheusTextExporter:
    """
    Minimal Prometheus text-format exporter for telemetryd.
    Uses only asyncio.start_server (no external dependencies).
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 9100) -> None:
        self._host = host
        self._port = port

        # Store latest metric values
        # Key: (host, metric_name)
        # Value: (value, rate, timestamp)
        self._metrics: dict[tuple[str, str], tuple[int, float, float]] = {}

        # Async server handle
        self._server: asyncio.AbstractServer | None = None

    @staticmethod
    def _sanitize_metric_name(name: str) -> str:
        """
        Sanitize metric name to Prometheus-compatible format:
        [a-zA-Z_:][a-zA-Z0-9_:]*
        """
        import re

        # Replace invalid chars with underscore
        name = re.sub(r"[^a-zA-Z0-9_:]", "_", name)

        # Ensure it starts with a letter or underscore
        if not name or not re.match(r"[a-zA-Z_]", name[0]):
            name = f"m_{name}"

        return name

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """
        Serve Prometheus text exposition format on /metrics.
        Very small HTTP parser: only supports GET /metrics.
        """
        try:
            request_line = await reader.readline()
            if not request_line:
                writer.close()
                await writer.wait_closed()
                return

            try:
                method, path, _ = request_line.decode("utf-8").strip().split(" ", 2)
            except ValueError:
                method, path = "GET", "/"

            # Drain remaining headers
            while True:
                line = await reader.readline()
                if not line or line in (b"\r\n", b"\n"):
                    break

            if method != "GET" or path != "/metrics":
                # Minimal 404
                writer.write(
                    b"HTTP/1.1 404 Not Found\r\n"
                    b"Content-Type: text/plain\r\n"
                    b"Content-Length: 9\r\n"
                    b"\r\n"
                    b"Not found"
                )
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return

            # Build Prometheus payload
            lines: list[str] = []

            # Optional HELP/TYPE lines per metric base name
            seen_bases: set[str] = set()

            for (host, name), (value, rate, ts) in self._metrics.items():
                metric_base_raw = f"telemetryd_{name}"
                metric_base = self._sanitize_metric_name(metric_base_raw)

                if metric_base not in seen_bases:
                    lines.append(
                        f"# HELP {metric_base}_value Raw counter value from telemetryd."
                    )
                    lines.append(f"# TYPE {metric_base}_value gauge")
                    lines.append(
                        f"# HELP {metric_base}_rate Per-second rate computed by telemetryd."
                    )
                    lines.append(f"# TYPE {metric_base}_rate gauge")
                    lines.append(
                        f"# HELP {metric_base}_timestamp Last update timestamp (seconds since epoch)."
                    )
                    lines.append(f"# TYPE {metric_base}_timestamp gauge")
                    seen_bases.add(metric_base)

                # Raw value
                lines.append(f'{metric_base}_value{{host="{host}"}} {value}')

                # Rate
                lines.append(f'{metric_base}_rate{{host="{host}"}} {rate}')

                # Timestamp
                lines.append(f'{metric_base}_timestamp{{host="{host}"}} {ts}')

            payload = "\n".join(lines) + "\n"

            # HTTP response
            body = payload.encode("utf-8")
            headers = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
                b"Content-Length: " + str(len(body)).encode("utf-8") + b"\r\n"
                b"Connection: close\r\n"
                b"\r\n"
            )

            writer.write(headers + body)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async def start_server(self) -> None:
        """Start the Prometheus HTTP endpoint."""
        self._server = await asyncio.start_server(
            self._handle_client, self._host, self._port
        )

    def startup(self, device_count: int, interval: float) -> None:
        # No-op for Prometheus
        pass

    def metric(self, host: str, response: SNMPResponse, rate: float) -> None:
        self._metrics[(host, response.name)] = (
            response.value,
            rate,
            time.time(),
        )

    def init_value(self, host: str, response: SNMPResponse) -> None:
        # Store initial value with rate=0.0
        self._metrics[(host, response.name)] = (
            response.value,
            0.0,
            time.time(),
        )

    def error(self, host: str, exc: Exception) -> None:
        # Prometheus exporters typically do not expose errors as metrics
        pass
