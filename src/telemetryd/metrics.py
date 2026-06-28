import logging
import time
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
    Corrected version:
    - Per-device LRU history (prevents cross-device eviction bugs)
    - Timestamped samples using monotonic clock
    - Robust wrap vs reset detection
    - GAUGE passthrough
    - Safe eviction for devices and OIDs
    """

    MAX_DEVICES = 1_000
    MAX_OIDS_PER_DEVICE = 200

    def __init__(self) -> None:
        # host → OrderedDict[oid → (value, timestamp)]
        self._history: dict[str, OrderedDict[str, tuple[int, float]]] = {}

    def _update_history(
        self, host: str, oid: str, value: int, timestamp: float
    ) -> tuple[int, float] | None:
        """Store current sample and return previous one."""
        # Ensure host bucket exists
        if host not in self._history:
            # Evict oldest host if needed
            if len(self._history) >= self.MAX_DEVICES:
                oldest_host = next(iter(self._history))
                self._history.pop(oldest_host)
            self._history[host] = OrderedDict()

        device_history = self._history[host]
        previous = device_history.get(oid)

        # Update LRU entry
        device_history[oid] = (value, timestamp)
        device_history.move_to_end(oid)

        # Enforce per-device OID limit
        while len(device_history) > self.MAX_OIDS_PER_DEVICE:
            device_history.popitem(last=False)

        return previous

    def calculate_rate(
        self, host: str, response: SNMPResponse, now: float | None = None
    ) -> float | None:
        """
        Compute per-second rate using monotonic timestamps.
        Returns:
            float — rate
            None  — first observation or dropped reset window
        """
        timestamp = now if now is not None else time.monotonic()
        current_value = response.value
        snmp_type = response.snmp_type.upper()

        # GAUGE: raw value passthrough
        if snmp_type == "GAUGE":
            return float(current_value)

        # Fetch previous sample
        previous = self._update_history(host, response.oid, current_value, timestamp)
        if previous is None:
            return None

        previous_value, previous_time = previous
        delta_time = timestamp - previous_time

        if delta_time <= 0:
            logger.warning(
                f"Invalid delta_time ({delta_time:.6f}) for {host}:{response.name}"
            )
            return 0.0

        raw_delta = current_value - previous_value

        # No change
        if raw_delta == 0:
            return 0.0

        # Handle wrap or reset
        if raw_delta < 0:
            if snmp_type in ("COUNTER32", "COUNTER"):
                max_val = 2**32
            elif snmp_type == "COUNTER64":
                max_val = 2**64
            else:
                logger.warning(
                    f"Negative delta on non-counter {snmp_type} for {host}:{response.name}"
                )
                return 0.0

            adjusted_delta = raw_delta + max_val

            # Reset detection heuristic
            if adjusted_delta > max_val * 0.95:
                logger.warning(
                    f"Counter reset detected on {host}:{response.name} "
                    f"(from {previous_value} → {current_value}). Dropping sample."
                )
                return None

            logger.info(
                f"{snmp_type} wraparound corrected for {host} [{response.name}]"
            )
        else:
            adjusted_delta = raw_delta

        rate = adjusted_delta / delta_time
        return round(max(rate, 0.0), 2)
