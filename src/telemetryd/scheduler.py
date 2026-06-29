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

        self._shutdown_event: asyncio.Event = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    def _load_config(self, path: Path) -> ProgramConfig:
        with open(path, "r") as f:
            return json.load(f)

    def request_shutdown(self) -> None:
        self._shutdown_event.set()
        for task in self._tasks:
            task.cancel()

    async def poll_device(self, device: DeviceConfig) -> None:
        host = device["host"]
        port = device["port"]
        community = device["community"]
        metrics_cfg = device["metrics"]

        try:
            responses = await self.client.fetch_metrics(
                host, port, community, metrics_cfg
            )

            arrival_time = time.monotonic()

            for resp in responses:
                rate = self.calculator.calculate_rate(
                    host, resp, current_time=arrival_time
                )
                if rate is not None:
                    self.reporter.metric(host, resp, rate)
                else:
                    self.reporter.init_value(host, resp)

        except Exception as e:
            self.reporter.error(host, e)

    async def _device_loop(self, device: DeviceConfig) -> None:
        host = device["host"]

        initial_delay = self.client._rng.uniform(0, self.interval)
        try:
            await asyncio.sleep(initial_delay)
        except asyncio.CancelledError:
            return

        while not self._shutdown_event.is_set():
            loop_start = time.monotonic()

            try:
                await asyncio.wait_for(self.poll_device(device), timeout=self.interval)
            except asyncio.TimeoutError:
                self.reporter.error(host, TimeoutError("poll timeout"))
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.reporter.error(host, e)

            elapsed = time.monotonic() - loop_start
            sleep_adjustment = max(0.0, self.interval - elapsed)

            try:
                await asyncio.sleep(sleep_adjustment)
            except asyncio.CancelledError:
                break

    async def run_once(self, devices: Iterable[DeviceConfig] | None = None) -> float:
        targets = list(devices) if devices is not None else self.devices
        start_loop = time.monotonic()
        tasks = [self.poll_device(device) for device in targets]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        return time.monotonic() - start_loop

    async def start(self) -> None:
        self.reporter.startup(len(self.devices), self.interval)

        self._tasks = [
            asyncio.create_task(self._device_loop(device)) for device in self.devices
        ]

        try:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass
