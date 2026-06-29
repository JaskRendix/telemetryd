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
    snmp_type: str  # 'COUNTER32', 'COUNTER64', 'COUNTER', or 'GAUGE'


class RateCalculator:
    """
    Hardened SNMP rate calculator with:
      • Correct wraparound handling
      • Correct reset detection
      • Protection against LRU poisoning
      • Protection against invalid timestamps
      • Optional physical‑rate sanity checks
      • Per‑device LRU caches
      • Deterministic behavior
    """

    MAX_DEVICES: int = 1_000
    MAX_OIDS_PER_DEVICE: int = 200

    # Optional: physical rate sanity limit (bytes/sec, packets/sec, etc.)
    # Set to None to disable.
    MAX_REASONABLE_RATE: float | None = None

    def __init__(self) -> None:
        self._history: dict[str, OrderedDict[str, tuple[int, float]]] = {}

    def _update_history(
        self, host: str, oid: str, value: int, timestamp: float
    ) -> tuple[int, float] | None:

        if host not in self._history:
            if len(self._history) >= self.MAX_DEVICES:
                oldest_host = next(iter(self._history))
                logger.debug(f"Evicting oldest host: {oldest_host}")
                self._history.pop(oldest_host)
            self._history[host] = OrderedDict()

        device_history = self._history[host]
        previous = device_history.get(oid)

        device_history[oid] = (value, timestamp)
        device_history.move_to_end(oid)

        while len(device_history) > self.MAX_OIDS_PER_DEVICE:
            evicted_oid, _ = device_history.popitem(last=False)
            logger.debug(f"Evicted OID {evicted_oid} from host {host}")

        return previous

    def calculate_rate(
        self,
        host: str,
        response: SNMPResponse,
        *,
        now: float | None = None,
        current_time: float | None = None,
    ) -> float | None:

        # Timestamp selection
        timestamp: float = (
            current_time
            if current_time is not None
            else now if now is not None else time.monotonic()
        )

        current_value: int = response.value
        snmp_type: str = response.snmp_type.upper()

        if snmp_type == "GAUGE":
            return float(current_value)

        # Normalize COUNTER alias
        if snmp_type == "COUNTER":
            snmp_type = "COUNTER32"

        device_history = self._history.get(host, {})
        previous = device_history.get(response.oid)

        if previous is not None:
            previous_value, previous_time = previous
            delta_time = timestamp - previous_time

            # Protect against invalid timestamp sequences
            if delta_time <= 0:
                logger.warning(
                    f"Non-positive time delta for host={host}, oid={response.oid}, "
                    f"name={response.name}. Dropping sample."
                )
                return 0.0

        previous = self._update_history(host, response.oid, current_value, timestamp)
        if previous is None:
            # First sample → no rate
            return None

        previous_value, previous_time = previous
        delta_time = timestamp - previous_time
        raw_delta = current_value - previous_value

        if raw_delta == 0:
            return 0.0

        if raw_delta < 0:
            if snmp_type == "COUNTER32":
                max_val = 2**32
            elif snmp_type == "COUNTER64":
                max_val = 2**64
            else:
                return 0.0

            adjusted_delta = raw_delta + max_val

            if current_value < (max_val * 0.05) and adjusted_delta > (max_val * 0.90):
                logger.info(
                    f"Counter reset detected on host={host}, oid={response.oid}, "
                    f"name={response.name}"
                )
                return None
        else:
            adjusted_delta = raw_delta

        rate = adjusted_delta / delta_time

        if self.MAX_REASONABLE_RATE is not None and rate > self.MAX_REASONABLE_RATE:
            logger.warning(
                f"Rate {rate} exceeds MAX_REASONABLE_RATE={self.MAX_REASONABLE_RATE} "
                f"for host={host}, oid={response.oid}. Dropping sample."
            )
            return None

        return round(max(rate, 0.0), 2)
