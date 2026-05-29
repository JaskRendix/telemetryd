import asyncio
import random

from telemetryd.metrics import SNMPResponse


class AsyncSNMPClient:
    """
    Asynchronous network helper wrapper.
    In production, this interfaces directly with async networking libraries like aiosnmp.
    Here, it simulates asynchronous I/O latency and real metrics.
    """

    def __init__(self) -> None:
        # State tracking to simulate monotonic metric accumulation per device
        self._mock_accumulators: dict[str, int] = {}

    async def fetch_metrics(
        self, host: str, port: int, community: str, metrics_config: list[dict]
    ) -> list[SNMPResponse]:
        # Simulate real-world network latency asynchronously
        await asyncio.sleep(random.uniform(0.05, 0.2))

        responses = []
        for metric in metrics_config:
            oid = metric["oid"]
            m_type = metric["type"]
            m_name = metric["name"]
            state_key = f"{host}:{oid}"

            if state_key not in self._mock_accumulators:
                # Initialize with a random baseline integer value
                self._mock_accumulators[state_key] = random.randint(1000, 50000)

            # Simulate variable output generation speed (e.g., printing pages or shifting bytes)
            increment = random.randint(5, 50)

            # Artificial edge-case testing: Force a 32-bit integer overflow occasionally
            if m_type == "COUNTER32" and random.random() > 0.98:
                self._mock_accumulators[state_key] = (
                    2**32 - 10
                )  # Prime it to overflow next loop

            # Apply accumulation value, wrapping securely past the architecture boundary limit
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
