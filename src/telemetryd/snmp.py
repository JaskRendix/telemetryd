import asyncio
import random
from collections.abc import Callable, Sequence

from telemetryd.metrics import SNMPResponse


class AsyncSNMPClient:
    """
    Asynchronous SNMP simulator with injectable randomness, jitter, failure modes,
    and optional realism extensions:

    - per‑OID latency overrides
    - per‑device jitter profiles
    - per‑OID failure rates
    - per‑device partial response profiles
    - configurable wrap frequency
    - SNMP type‑specific increment distributions
    """

    def __init__(
        self,
        rng: random.Random | None = None,
        latency_range: Sequence[float] = (0.05, 0.2),
        failure_rate: float = 0.0,
        malformed_rate: float = 0.0,
        partial_rate: float = 0.0,
        jitter_rate: float = 0.0,
        jitter_range: Sequence[float] = (0.2, 0.5),
        per_oid_latency: dict[str, tuple[float, float]] | None = None,
        per_device_latency: dict[str, tuple[float, float]] | None = None,
        per_oid_failure: dict[str, float] | None = None,
        per_device_partial: dict[str, float] | None = None,
        per_device_jitter: dict[str, tuple[float, float]] | None = None,
        wrap_frequency: float = 0.02,
        increment_distributions: (
            dict[str, Callable[[random.Random], int]] | None
        ) = None,
    ) -> None:

        self._rng = rng or random.Random()

        # Global latency/jitter defaults
        self._latency_min, self._latency_max = map(float, latency_range)
        self._jitter_min, self._jitter_max = map(float, jitter_range)

        # Failure modes
        self._failure_rate = float(failure_rate)
        self._malformed_rate = float(malformed_rate)
        self._partial_rate = float(partial_rate)
        self._jitter_rate = float(jitter_rate)

        # Realism knobs
        self._per_oid_latency = per_oid_latency or {}
        self._per_device_latency = per_device_latency or {}
        self._per_oid_failure = per_oid_failure or {}
        self._per_device_partial = per_device_partial or {}
        self._per_device_jitter = per_device_jitter or {}
        self._wrap_frequency = float(wrap_frequency)

        # Increment distributions per SNMP type
        self._increment_distributions = increment_distributions or {}

        # Internal counters
        self._mock_accumulators: dict[str, int] = {}

    async def fetch_metrics(
        self,
        host: str,
        port: int,
        community: str,
        metrics_config: list[dict],
    ) -> list[SNMPResponse]:

        # --- DEVICE‑LEVEL LATENCY OVERRIDE ---
        if host in self._per_device_latency:
            lo, hi = self._per_device_latency[host]
            await asyncio.sleep(self._rng.uniform(lo, hi))
        else:
            # --- PER‑OID LATENCY OVERRIDE ---
            oid_latencies = [
                self._per_oid_latency.get(m["oid"])
                for m in metrics_config
                if m["oid"] in self._per_oid_latency
            ]
            if oid_latencies:
                lo, hi = max(oid_latencies, key=lambda x: x[1])
                await asyncio.sleep(self._rng.uniform(lo, hi))
            else:
                await asyncio.sleep(
                    self._rng.uniform(self._latency_min, self._latency_max)
                )

        # --- DEVICE‑LEVEL JITTER PROFILE ---
        if host in self._per_device_jitter:
            lo, hi = self._per_device_jitter[host]
            await asyncio.sleep(self._rng.uniform(lo, hi))
        else:
            if self._rng.random() < self._jitter_rate:
                await asyncio.sleep(
                    self._rng.uniform(self._jitter_min, self._jitter_max)
                )

        # --- GLOBAL FAILURE MODES ---
        if self._rng.random() < self._failure_rate:
            raise TimeoutError(f"Simulated timeout for {host}")

        if self._rng.random() < self._malformed_rate:
            raise ValueError(f"Malformed SNMP response from {host}")

        # --- DEVICE‑LEVEL PARTIAL RESPONSE PROFILE ---
        effective_partial_rate = self._per_device_partial.get(host, self._partial_rate)
        if self._rng.random() < effective_partial_rate:
            metrics_config = metrics_config[: max(1, len(metrics_config) // 2)]

        responses: list[SNMPResponse] = []

        for metric in metrics_config:
            oid = metric["oid"]
            m_type = metric["type"]
            m_name = metric["name"]
            state_key = f"{host}:{oid}"

            # --- PER‑OID FAILURE RATE ---
            if oid in self._per_oid_failure:
                if self._rng.random() < self._per_oid_failure[oid]:
                    raise TimeoutError(
                        f"Simulated OID‑specific failure for {host}:{oid}"
                    )

            # Initialize accumulator
            if state_key not in self._mock_accumulators:
                self._mock_accumulators[state_key] = self._rng.randint(1000, 50000)

            # --- TYPE‑SPECIFIC INCREMENT DISTRIBUTION ---
            if m_type in self._increment_distributions:
                increment = self._increment_distributions[m_type](self._rng)
            else:
                increment = self._rng.randint(5, 50)

            # --- CONFIGURABLE WRAP FREQUENCY ---
            if m_type == "COUNTER32" and self._rng.random() < self._wrap_frequency:
                self._mock_accumulators[state_key] = 2**32 - 10

            # Apply increment
            self._mock_accumulators[state_key] += increment

            # Apply wrap
            if m_type == "COUNTER32":
                self._mock_accumulators[state_key] %= 2**32
            elif m_type == "COUNTER64":
                self._mock_accumulators[state_key] %= 2**64

            responses.append(
                SNMPResponse(
                    oid=oid,
                    name=m_name,
                    value=self._mock_accumulators[state_key],
                    snmp_type=m_type,
                )
            )

        return responses
