import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SNMPResponse:
    oid: str
    name: str
    value: int
    snmp_type: str  # 'COUNTER32' or 'COUNTER64'


class RateCalculator:
    def __init__(self) -> None:
        # Maps (host, oid) -> previous_integer_value
        self._history: dict[tuple[str, str], int] = {}

    def calculate_rate(
        self, host: str, response: SNMPResponse, delta_time: float
    ) -> float | None:
        key = (host, response.oid)
        current_value = response.value
        previous_value = self._history.get(key)

        # Update history cache immediately for the next iteration
        self._history[key] = current_value

        if previous_value is None:
            # First poll data point; cannot compute a delta rate yet
            return None

        if delta_time <= 0:
            logger.warning(
                f"Invalid delta_time ({delta_time}) for {host}:{response.name}"
            )
            return 0.0

        raw_delta = current_value - previous_value

        # Handle counter wraparound using Python 3.12 pattern matching
        if raw_delta < 0:
            match response.snmp_type.upper():
                case "COUNTER32" | "COUNTER":
                    adjusted_delta = raw_delta + (2**32)
                    logger.info(
                        f"Counter32 overflow detected and corrected for {host} [{response.name}]"
                    )
                case "COUNTER64":
                    adjusted_delta = raw_delta + (2**64)
                    logger.info(
                        f"Counter64 overflow detected and corrected for {host} [{response.name}]"
                    )
                case _:
                    logger.error(
                        f"Negative delta with unhandled type {response.snmp_type} for {host}"
                    )
                    return 0.0
        else:
            adjusted_delta = raw_delta

        return round(adjusted_delta / delta_time, 2)
