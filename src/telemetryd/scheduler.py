import asyncio
import json
import logging
import time
from collections.abc import Iterable
from pathlib import Path

from telemetryd.metrics import RateCalculator, SNMPResponse
from telemetryd.snmp import AsyncSNMPClient

type DeviceConfig = dict
type ProgramConfig = dict


class TelemetryReporter:
    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)

    def startup(self, device_count: int, interval: float) -> None:
        self._logger.info(
            f"Starting Printer Telemetry Daemon with {device_count} targets. Polling every {interval}s."
        )

    def metric(self, host: str, response: SNMPResponse, rate: float) -> None:
        self._logger.info(
            f"[METRIC] Host: {host:<15} | Key: {response.name:<13} | "
            f"Value: {response.value:<11} | Rate: {rate:>6} units/sec"
        )

    def init_value(self, host: str, response: SNMPResponse) -> None:
        self._logger.info(
            f"[INIT] Host: {host:<15} | Key: {response.name:<13} | "
            f"Base Value: {response.value}"
        )

    def error(self, host: str, exc: Exception) -> None:
        self._logger.error(f"Error polling host {host}: {exc}")


class TelemetryDaemon:
    def __init__(
        self,
        config_path: Path,
        client: AsyncSNMPClient | None = None,
        calculator: RateCalculator | None = None,
        reporter: TelemetryReporter | None = None,
    ) -> None:
        self.config: ProgramConfig = self._load_config(config_path)
        self.interval: float = self.config.get("polling_interval_seconds", 5.0)
        self.devices: list[DeviceConfig] = self.config.get("devices", [])

        self.client = client or AsyncSNMPClient()
        self.calculator = calculator or RateCalculator()
        self.reporter = reporter or TelemetryReporter(logging.getLogger(__name__))

        self._last_poll_time: dict[str, float] = {}
        self._shutdown_event: asyncio.Event = asyncio.Event()

    def _load_config(self, path: Path) -> ProgramConfig:
        with open(path, "r") as f:
            return json.load(f)

    def request_shutdown(self) -> None:
        self._shutdown_event.set()

    async def poll_device(self, device: DeviceConfig) -> None:
        host = device["host"]
        port = device["port"]
        community = device["community"]
        metrics_cfg = device["metrics"]

        current_time = time.time()
        previous_time = self._last_poll_time.get(host)
        delta_time = current_time - previous_time if previous_time else 0.0
        self._last_poll_time[host] = current_time

        try:
            responses = await self.client.fetch_metrics(
                host, port, community, metrics_cfg
            )

            for resp in responses:
                rate = self.calculator.calculate_rate(host, resp, delta_time)
                if rate is not None:
                    self.reporter.metric(host, resp, rate)
                else:
                    self.reporter.init_value(host, resp)
        except Exception as e:
            self.reporter.error(host, e)

    async def run_once(self, devices: Iterable[DeviceConfig] | None = None) -> float:
        targets = list(devices) if devices is not None else self.devices
        start_loop = time.time()
        tasks = [self.poll_device(device) for device in targets]
        if tasks:
            await asyncio.gather(*tasks)
        execution_duration = time.time() - start_loop
        return execution_duration

    async def start(self) -> None:
        self.reporter.startup(len(self.devices), self.interval)

        while not self._shutdown_event.is_set():
            execution_duration = await self.run_once()
            sleep_adjustment = max(0.0, self.interval - execution_duration)
            if sleep_adjustment > 0:
                await asyncio.sleep(sleep_adjustment)
