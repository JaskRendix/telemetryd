import json
from unittest.mock import patch

import pytest

from telemetryd.metrics import SNMPResponse
from telemetryd.scheduler import TelemetryDaemon, TelemetryReporter


@pytest.mark.asyncio
async def test_full_integration_poll_cycle(tmp_path):
    cfg = {
        "polling_interval_seconds": 0.01,
        "devices": [
            {
                "host": "h",
                "port": 161,
                "community": "c",
                "metrics": [
                    {"oid": "1", "type": "COUNTER32", "name": "m"},
                    {"oid": "2", "type": "COUNTER64", "name": "x"},
                ],
            }
        ],
    }

    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(cfg))

    reporter = TelemetryReporter()
    d = TelemetryDaemon(p, reporter=reporter)

    seq = [
        [
            SNMPResponse("1", "m", 100, "COUNTER32"),
            SNMPResponse("2", "x", 200, "COUNTER64"),
        ],
        [
            SNMPResponse("1", "m", 160, "COUNTER32"),
            SNMPResponse("2", "x", 260, "COUNTER64"),
        ],
    ]

    async def fake_fetch(*args, **kwargs):
        return seq.pop(0)

    with patch.object(d.client, "fetch_metrics", side_effect=fake_fetch):
        # Patch time.time() so the daemon records deterministic timestamps
        with patch("telemetryd.scheduler.time.time", side_effect=[100.0, 102.0]):
            await d.poll_device(cfg["devices"][0])
            await d.poll_device(cfg["devices"][0])

    v1, ts1 = d.calculator._history["h"]["1"]
    v2, ts2 = d.calculator._history["h"]["2"]

    assert v1 == 160
    assert v2 == 260

    # Last poll time should be the patched timestamp
    assert d._last_poll_time["h"] == 102.0
