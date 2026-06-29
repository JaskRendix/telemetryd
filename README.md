# telemetryd

`telemetryd` is an asynchronous SNMP polling daemon for collecting printer counters and computing rate‑based metrics. It polls devices concurrently, handles counter wraparound, and emits raw values and computed rates.

---

## Features

- concurrent polling  
- per‑second rate calculation  
- COUNTER32 and COUNTER64 wraparound handling  
- GAUGE support  
- deterministic behavior with injected RNG  
- fixed‑interval scheduling with drift control  
- per‑device timeouts  
- staggered initial polls  
- pluggable reporters (JSON, CSV, Prometheus)

---

## Installation

Editable install:

```
pip install -e .
```

Test dependencies:

```
pip install .[test]
```

Run tests:

```
pytest
```

---

## Architecture

### Scheduler
Runs the polling loop.

- loads configuration  
- applies staggering  
- wraps each poll in a timeout  
- maintains a fixed interval  
- supports graceful shutdown  
- exposes `run_once` for tests  

### SNMP client
Simulates SNMP polling with deterministic behavior and optional realism profiles.

- base latency  
- jitter spikes  
- partial metric sets  
- malformed responses  
- deterministic behavior with injected RNG  
- per‑OID latency overrides  
- per‑device latency profiles  
- per‑device jitter profiles  
- per‑OID failure rates  
- per‑device partial‑response rates  
- configurable wrap frequency for COUNTER32  
- SNMP type‑specific increment distributions  

These options allow controlled simulation of degraded networks, slow devices, flaky OIDs, and realistic counter behavior. All realism knobs are optional; if not provided, the client behaves like a simple deterministic SNMP simulator.

### Rate calculator
Computes per‑second rates.

- tracks previous values per `(host, oid)`  
- handles wraparound  
- enforces monotonicity  
- supports GAUGE  
- uses LRU eviction  

### Reporters
Receive:

- initial values  
- computed rates  
- polling errors  

Built‑in reporters:

- JSON (NDJSON)  
- CSV  
- Prometheus text exporter  

### Configuration
`config.json` defines:

- polling interval  
- devices  
- SNMP community  
- OIDs and SNMP types  

---

## Directory layout

```
telemetryd/
│
├── config.json
├── pyproject.toml
├── README.md
│
├── src/
│   └── telemetryd/
│       ├── __init__.py
│       ├── main.py
│       ├── metrics.py
│       ├── scheduler.py
│       ├── snmp.py
│       └── ext/
│           ├── json_reporter.py
│           ├── csv_reporter.py
│           └── prometheus_exporter.py
│
└── tests/
    ├── test_main.py
    ├── test_chaos.py
    ├── test_daemon.py
    ├── test_daemon_concurrency.py
    ├── test_daemon_staggering.py
    ├── test_integration_daemon.py
    ├── test_metrics.py
    └── test_snmp_client.py
```

---

## Example configuration

```json
{
  "polling_interval_seconds": 2.0,
  "devices": [
    {
      "host": "192.168.1.100",
      "port": 161,
      "community": "public",
      "metrics": [
        {"oid": "1.3.6.1.2.1.43.10.2.1.4.1.1", "type": "COUNTER32", "name": "total_pages"},
        {"oid": "1.3.6.1.2.1.2.2.1.10.1", "type": "COUNTER64", "name": "if_in_octets"}
      ]
    }
  ]
}
```

---

## Running the daemon

After installation, the package exposes a `telemetryd` command.

### Reporter selection

```
telemetryd --reporter json
telemetryd --reporter csv
telemetryd --reporter prometheus
```

### Custom configuration

```
telemetryd --config config.json --reporter csv
```

### Reporters

**JSONReporter**  
Writes NDJSON to `logs/telemetry.jsonl`.

**CSVReporter**  
Writes CSV rows to `logs/telemetry.csv`.

**PrometheusTextExporter**  
Provides a `/metrics` HTTP endpoint on port 9100 in Prometheus text format, exposing raw counter values, per‑second rates, and timestamps for scraping.

---

### HTTP health endpoint
Telemetryd includes a small HTTP listener that exposes two routes:

- `/health` — returns `200 OK`
- `/ready` — returns `200 OK` after the first successful poll, otherwise `503 Service Unavailable`

The listener runs on port `8081` and uses a minimal TCP server. It provides basic liveness and readiness signals for process supervisors and orchestration systems.
