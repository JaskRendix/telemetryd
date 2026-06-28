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
    A robust per‑device, per‑OID rate calculator for SNMP counters.

    Features
    --------
    • Maintains a **per‑device LRU history** of OID → (value, timestamp)
      to avoid cross‑device eviction bugs.

    • Supports both legacy `now=` and new `current_time=` parameters.
      Only one should be provided; if both are None, monotonic time is used.

    • Correct handling of:
        - COUNTER32 / COUNTER64 wraparound
        - counter resets (large negative deltas)
        - GAUGE passthrough (returns raw value)
        - zero‑delta (returns 0.0)
        - non‑monotonic timestamps (returns 0.0)

    • Enforces:
        - MAX_DEVICES: maximum number of device histories
        - MAX_OIDS_PER_DEVICE: maximum number of OIDs per device history

    Return Value Semantics
    ----------------------
    • `None`:
        - First observation of an OID
        - Counter reset detected
        - GAUGE negative delta (treated as raw value)
        - Invalid sample window

    • `float`:
        - Computed rate in units/sec
        - Rounded to 2 decimals
        - Always >= 0.0

    Internal State
    --------------
    `_history` structure:
        {
            host: OrderedDict[
                oid: (value: int, timestamp: float)
            ]
        }

    This ensures predictable eviction order and stable memory usage.
    """

    MAX_DEVICES: int = 1_000
    MAX_OIDS_PER_DEVICE: int = 200

    # host → OrderedDict[oid → (value, timestamp)]
    _history: dict[str, OrderedDict[str, tuple[int, float]]]

    def __init__(self) -> None:
        self._history = {}

    def _update_history(
        self, host: str, oid: str, value: int, timestamp: float
    ) -> tuple[int, float] | None:
        """
        Insert or update the (value, timestamp) entry for a given host+OID.

        Returns
        -------
        previous : (value, timestamp) or None
            The previous sample if it existed, else None.
        """
        if host not in self._history:
            # Evict oldest host if needed
            if len(self._history) >= self.MAX_DEVICES:
                oldest_host = next(iter(self._history))
                self._history.pop(oldest_host)
            self._history[host] = OrderedDict()

        device_history = self._history[host]
        previous = device_history.get(oid)

        device_history[oid] = (value, timestamp)
        device_history.move_to_end(oid)

        # Enforce per‑device OID limit
        while len(device_history) > self.MAX_OIDS_PER_DEVICE:
            device_history.popitem(last=False)

        return previous

    def calculate_rate(
        self,
        host: str,
        response: SNMPResponse,
        *,
        now: float | None = None,
        current_time: float | None = None,
    ) -> float | None:
        """
        Compute the per‑second rate for an SNMP counter or gauge.

        Parameters
        ----------
        host : str
            Device identifier.
        response : SNMPResponse
            The SNMP metric sample.
        now : float, optional
            Legacy timestamp parameter (seconds). If provided, overrides monotonic().
        current_time : float, optional
            New timestamp parameter (seconds). If provided, overrides `now`.

        Returns
        -------
        float or None
            • None for first sample, resets, invalid windows.
            • Float rate (>= 0.0) for valid counter deltas.
            • GAUGE returns raw value.
        """
        timestamp: float = (
            current_time
            if current_time is not None
            else now if now is not None else time.monotonic()
        )

        current_value: int = response.value
        snmp_type: str = response.snmp_type.upper()

        # GAUGE: raw value passthrough
        if snmp_type == "GAUGE":
            return float(current_value)

        previous = self._update_history(host, response.oid, current_value, timestamp)
        if previous is None:
            return None

        previous_value, previous_time = previous
        delta_time: float = timestamp - previous_time

        if delta_time <= 0:
            return 0.0

        raw_delta: int = current_value - previous_value

        if raw_delta == 0:
            return 0.0

        # Handle wrap or reset
        if raw_delta < 0:
            if snmp_type in ("COUNTER32", "COUNTER"):
                max_val = 2**32
            elif snmp_type == "COUNTER64":
                max_val = 2**64
            else:
                return 0.0

            adjusted_delta = raw_delta + max_val

            # Reset detection heuristic
            if adjusted_delta > max_val * 0.95:
                return None
        else:
            adjusted_delta = raw_delta

        rate: float = adjusted_delta / delta_time
        return round(max(rate, 0.0), 2)
