import asyncio
import random
from collections.abc import Sequence

from telemetryd.metrics import SNMPResponse


class AsyncSNMPClient:
    """
    Asynchronous network helper wrapper.
    In production, this interfaces directly with async networking libraries like aiosnmp.
    Here, it simulates asynchronous I/O latency and real metrics.
    """

    def __init__(
        self,
        rng: random.Random | None = None,
        latency_range: Sequence[float] = (0.05, 0.2),
    ) -> None:
        self._rng: random.Random = rng or random.Random()
        self._latency_min, self._latency_max = float(latency_range[0]), float(
            latency_range[1]
        )
        self._mock_accumulators: dict[str, int] = {}

    async def fetch_metrics(
        self, host: str, port: int, community: str, metrics_config: list[dict]
    ) -> list[SNMPResponse]:
        await asyncio.sleep(self._rng.uniform(self._latency_min, self._latency_max))

        responses: list[SNMPResponse] = []
        for metric in metrics_config:
            oid = metric["oid"]
            m_type = metric["type"]
            m_name = metric["name"]
            state_key = f"{host}:{oid}"

            if state_key not in self._mock_accumulators:
                self._mock_accumulators[state_key] = self._rng.randint(1000, 50000)

            increment = self._rng.randint(5, 50)

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
