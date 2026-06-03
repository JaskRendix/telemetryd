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
        self._server = None

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Serve Prometheus text exposition format."""
        lines = []

        for (host, name), (value, rate, ts) in self._metrics.items():
            metric_base = f"telemetryd_{name}"

            # Raw value
            lines.append(f'{metric_base}_value{{host="{host}"}} {value}')

            # Rate
            lines.append(f'{metric_base}_rate{{host="{host}"}} {rate}')

            # Timestamp (optional)
            lines.append(f'{metric_base}_timestamp{{host="{host}"}} {ts}')

        payload = "\n".join(lines) + "\n"
        writer.write(payload.encode("utf-8"))
        await writer.drain()
        writer.close()

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
