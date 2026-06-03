import json
import time
from pathlib import Path

from telemetryd.metrics import SNMPResponse


class JSONReporter:
    """
    A simple JSON sink for telemetryd.
    Writes each metric event as a JSON line (NDJSON format).
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

        # Open file in append mode
        self._fh = self._path.open("a", encoding="utf-8")

    def _write(self, payload: dict) -> None:
        json.dump(payload, self._fh)
        self._fh.write("\n")
        self._fh.flush()

    def startup(self, device_count: int, interval: float) -> None:
        self._write(
            {
                "event": "startup",
                "timestamp": time.time(),
                "device_count": device_count,
                "interval": interval,
            }
        )

    def metric(self, host: str, response: SNMPResponse, rate: float) -> None:
        self._write(
            {
                "event": "metric",
                "timestamp": time.time(),
                "host": host,
                "oid": response.oid,
                "name": response.name,
                "value": response.value,
                "rate": rate,
                "snmp_type": response.snmp_type,
            }
        )

    def init_value(self, host: str, response: SNMPResponse) -> None:
        self._write(
            {
                "event": "init",
                "timestamp": time.time(),
                "host": host,
                "oid": response.oid,
                "name": response.name,
                "value": response.value,
                "snmp_type": response.snmp_type,
            }
        )

    def error(self, host: str, exc: Exception) -> None:
        self._write(
            {
                "event": "error",
                "timestamp": time.time(),
                "host": host,
                "error": str(exc),
            }
        )

    def close(self) -> None:
        self._fh.close()
