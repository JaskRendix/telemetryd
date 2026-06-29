import argparse
import asyncio
import logging
from collections.abc import Callable
from pathlib import Path

from telemetryd.ext.csv_reporter import CSVReporter
from telemetryd.ext.json_reporter import JSONReporter
from telemetryd.ext.prometheus_exporter import PrometheusTextExporter
from telemetryd.scheduler import TelemetryDaemon

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("telemetryd")

ReporterFactory = Callable[[], object]

REPORTERS: dict[str, ReporterFactory] = {
    "json": lambda: JSONReporter("logs/telemetry.jsonl"),
    "csv": lambda: CSVReporter("logs/telemetry.csv"),
    "prometheus": lambda: PrometheusTextExporter(port=9100),
}


def build_reporter(kind: str):
    """
    Return a reporter instance for the given kind.

    Parameters
    ----------
    kind : str
        One of: "json", "csv", "prometheus".

    Returns
    -------
    object
        Reporter instance.

    Raises
    ------
    ValueError
        If the reporter type is unknown.
    """
    try:
        return REPORTERS[kind]()
    except KeyError:
        raise ValueError(f"Unknown reporter type: {kind}") from None


async def main() -> None:
    """
    Entry point for the telemetryd daemon.

    Responsibilities:
    - Parse CLI arguments
    - Validate configuration path
    - Build reporter
    - Start Prometheus server if needed
    - Launch TelemetryDaemon
    """
    parser = argparse.ArgumentParser(description="Telemetryd SNMP polling daemon")
    parser.add_argument(
        "--reporter",
        choices=list(REPORTERS.keys()),
        default="json",
        help="Select output reporter",
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to configuration file",
    )

    args = parser.parse_args()

    logger.info(f"Selected reporter: {args.reporter}")
    logger.info(f"Loading configuration from: {args.config}")

    config_file = Path(args.config)
    if not config_file.exists():
        logger.error(f"Configuration file not found: {config_file}")
        raise FileNotFoundError(f"Configuration file not found: {config_file}")

    reporter = build_reporter(args.reporter)

    if isinstance(reporter, PrometheusTextExporter):
        logger.info("Starting Prometheus exporter on port 9100")
        await reporter.start_server()

    logger.info("Initializing TelemetryDaemon")
    daemon = TelemetryDaemon(config_file, reporter=reporter)

    logger.info("Starting TelemetryDaemon polling loop")
    await daemon.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Daemon terminated by user")
