import csv
import time
from pathlib import Path

from telemetryd.metrics import SNMPResponse


class CSVReporter:
    """
    A simple CSV sink for telemetryd.
    Writes each telemetry event as a CSV row.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

        # Open file in append mode
        self._fh = self._path.open("a", newline="", encoding="utf-8")
        self._writer = csv.writer(self._fh)

        # Write header only if file is empty
        if self._path.stat().st_size == 0:
            self._writer.writerow(
                [
                    "timestamp",
                    "event",
                    "host",
                    "oid",
                    "name",
                    "value",
                    "rate",
                    "snmp_type",
                    "error",
                ]
            )
            self._fh.flush()

    def _write(self, row: list) -> None:
        self._writer.writerow(row)
        self._fh.flush()

    def startup(self, device_count: int, interval: float) -> None:
        self._write(
            [
                time.time(),
                "startup",
                "",
                "",
                "",
                device_count,
                interval,
                "",
                "",
            ]
        )

    def metric(self, host: str, response: SNMPResponse, rate: float) -> None:
        self._write(
            [
                time.time(),
                "metric",
                host,
                response.oid,
                response.name,
                response.value,
                rate,
                response.snmp_type,
                "",
            ]
        )

    def init_value(self, host: str, response: SNMPResponse) -> None:
        self._write(
            [
                time.time(),
                "init",
                host,
                response.oid,
                response.name,
                response.value,
                "",
                response.snmp_type,
                "",
            ]
        )

    def error(self, host: str, exc: Exception) -> None:
        self._write(
            [
                time.time(),
                "error",
                host,
                "",
                "",
                "",
                "",
                "",
                str(exc),
            ]
        )

    def close(self) -> None:
        self._fh.close()
