import asyncio
from pathlib import Path

from telemetryd.scheduler import TelemetryDaemon

if __name__ == "__main__":
    config_file = Path("config.json")
    daemon = TelemetryDaemon(config_file)

    try:
        asyncio.run(daemon.start())
    except KeyboardInterrupt:
        print("\nDaemon execution cleanly terminated via user input interface signals.")
