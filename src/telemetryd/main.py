import argparse
import asyncio
from pathlib import Path

from telemetryd.ext.csv_reporter import CSVReporter
from telemetryd.ext.json_reporter import JSONReporter
from telemetryd.ext.prometheus_exporter import PrometheusTextExporter
from telemetryd.scheduler import TelemetryDaemon


def build_reporter(kind: str):
    if kind == "json":
        return JSONReporter("logs/telemetry.jsonl")
    if kind == "csv":
        return CSVReporter("logs/telemetry.csv")
    if kind == "prometheus":
        return PrometheusTextExporter(port=9100)
    raise ValueError(f"Unknown reporter type: {kind}")


async def main():
    parser = argparse.ArgumentParser(description="Telemetryd SNMP polling daemon")
    parser.add_argument(
        "--reporter",
        choices=["json", "csv", "prometheus"],
        default="json",
        help="Select output reporter",
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to configuration file",
    )

    args = parser.parse_args()

    config_file = Path(args.config)
    reporter = build_reporter(args.reporter)

    if isinstance(reporter, PrometheusTextExporter):
        await reporter.start_server()

    daemon = TelemetryDaemon(config_file, reporter=reporter)
    await daemon.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nDaemon terminated.")
