import asyncio
import random
from collections.abc import Sequence

from telemetryd.metrics import SNMPResponse


class AsyncSNMPClient:
    """
    Asynchronous SNMP simulator with injectable randomness, jitter, and failure modes.
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
    ) -> None:
        self._rng = rng or random.Random()

        self._latency_min, self._latency_max = map(float, latency_range)
        self._jitter_min, self._jitter_max = map(float, jitter_range)

        self._failure_rate = float(failure_rate)
        self._malformed_rate = float(malformed_rate)
        self._partial_rate = float(partial_rate)
        self._jitter_rate = float(jitter_rate)

        self._mock_accumulators: dict[str, int] = {}

    async def fetch_metrics(
        self, host: str, port: int, community: str, metrics_config: list[dict]
    ) -> list[SNMPResponse]:

        # Base latency
        await asyncio.sleep(self._rng.uniform(self._latency_min, self._latency_max))

        # Jitter spike
        if self._rng.random() < self._jitter_rate:
            await asyncio.sleep(self._rng.uniform(self._jitter_min, self._jitter_max))

        # Simulated timeout
        if self._rng.random() < self._failure_rate:
            raise TimeoutError(f"Simulated timeout for {host}")

        # Simulated malformed response
        if self._rng.random() < self._malformed_rate:
            raise ValueError(f"Malformed SNMP response from {host}")

        # Simulated partial metric set
        if self._rng.random() < self._partial_rate:
            metrics_config = metrics_config[: max(1, len(metrics_config) // 2)]

        responses: list[SNMPResponse] = []

        for metric in metrics_config:
            oid = metric["oid"]
            m_type = metric["type"]
            m_name = metric["name"]
            state_key = f"{host}:{oid}"

            if state_key not in self._mock_accumulators:
                self._mock_accumulators[state_key] = self._rng.randint(1000, 50000)

            increment = self._rng.randint(5, 50)

            # Force overflow occasionally
            if m_type == "COUNTER32" and self._rng.random() > 0.98:
                self._mock_accumulators[state_key] = 2**32 - 10

            self._mock_accumulators[state_key] += increment

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
