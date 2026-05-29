import logging
from collections import OrderedDict
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SNMPResponse:
    oid: str
    name: str
    value: int
    snmp_type: str  # 'COUNTER32', 'COUNTER64', or 'GAUGE'


class RateCalculator:
    """
    Improvements in Step 4:
    - Monotonicity guarantees: negative deltas only allowed for wraparound
    - Gauge support: GAUGE returns raw value, no delta
    - History eviction: prevent unbounded memory growth
    """

    # Maximum number of (host, oid) entries to keep
    MAX_HISTORY = 10_000

    def __init__(self) -> None:
        # Use OrderedDict for LRU eviction
        self._history: OrderedDict[tuple[str, str], int] = OrderedDict()

    def _evict_if_needed(self) -> None:
        """Evict oldest entries if history grows too large."""
        while len(self._history) > self.MAX_HISTORY:
            self._history.popitem(last=False)

    def calculate_rate(
        self, host: str, response: SNMPResponse, delta_time: float
    ) -> float | None:
        key = (host, response.oid)
        current_value = response.value
        previous_value = self._history.get(key)

        # Move key to end (LRU behavior)
        self._history[key] = current_value
        self._history.move_to_end(key)

        # Evict old entries
        self._evict_if_needed()

        # GAUGE: always return raw value, even on first poll
        if response.snmp_type.upper() == "GAUGE":
            return float(current_value)

        # First observation for counters → no rate
        if previous_value is None:
            return None

        # Invalid delta_time
        if delta_time <= 0:
            logger.warning(
                f"Invalid delta_time ({delta_time}) for {host}:{response.name}"
            )
            return 0.0

        raw_delta = current_value - previous_value

        # Handle wraparound or reset
        if raw_delta < 0:
            match response.snmp_type.upper():
                case "COUNTER32" | "COUNTER":
                    adjusted_delta = raw_delta + (2**32)
                    logger.info(
                        f"Counter32 wraparound corrected for {host} [{response.name}]"
                    )
                case "COUNTER64":
                    adjusted_delta = raw_delta + (2**64)
                    logger.info(
                        f"Counter64 wraparound corrected for {host} [{response.name}]"
                    )
                case _:
                    # Negative delta on non-counter → treat as reset
                    logger.warning(
                        f"Negative delta for non-counter {response.snmp_type} on {host}:{response.name}"
                    )
                    return 0.0
        else:
            adjusted_delta = raw_delta

        # Monotonicity guarantee
        rate = adjusted_delta / delta_time
        return round(max(rate, 0.0), 2)
