import asyncio
import json
import logging
import time
from pathlib import Path

from telemetryd.metrics import RateCalculator
from telemetryd.snmp import AsyncSNMPClient

type DeviceConfig = dict
type ProgramConfig = dict

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class TelemetryDaemon:
    def __init__(self, config_path: Path) -> None:
        self.config: ProgramConfig = self._load_config(config_path)
        self.client = AsyncSNMPClient()
        self.calculator = RateCalculator()
        self._last_poll_time: dict[str, float] = {}

    def _load_config(self, path: Path) -> ProgramConfig:
        with open(path, "r") as f:
            return json.load(f)

    async def poll_device(self, device: DeviceConfig) -> None:
        host = device["host"]
        port = device["port"]
        community = device["community"]
        metrics_cfg = device["metrics"]

        current_time = time.time()
        # Calculate time delta specific to this distinct hardware target
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
                    logger.info(
                        f"[METRIC] Host: {host:<15} | Key: {resp.name:<13} | Value: {resp.value:<11} | Rate: {rate:>6} units/sec"
                    )
                else:
                    logger.info(
                        f"[INIT] Host: {host:<15} | Key: {resp.name:<13} | Base Value: {resp.value}"
                    )
        except Exception as e:
            logger.error(f"Error polling host {host}: {str(e)}")

    async def start(self) -> None:
        interval = self.config.get("polling_interval_seconds", 5.0)
        devices = self.config.get("devices", [])

        logger.info(
            f"Starting Printer Telemetry Daemon with {len(devices)} targets. Polling every {interval}s."
        )

        while True:
            start_loop = time.time()

            # Fire all async network requests concurrently using gather
            tasks = [self.poll_device(device) for device in devices]
            await asyncio.gather(*tasks)

            # High-precision drift calculation tracking
            execution_duration = time.time() - start_loop
            sleep_adjustment = max(0.0, interval - execution_duration)
            await asyncio.sleep(sleep_adjustment)
