# telemetryd

`telemetryd` is an asynchronous SNMP polling daemon for collecting printer counters and computing rate‑based metrics. It polls devices concurrently, handles counter wraparound, and emits raw values and computed rates.

---

## Overview

The daemon targets printers and other SNMP‑exposed devices that publish monotonic counters. It:

- polls devices concurrently  
- computes deltas and per‑second rates  
- handles COUNTER32 and COUNTER64 wraparound  
- staggers initial polls  
- enforces per‑device timeouts  
- maintains a stable polling interval through drift compensation  
- supports deterministic behavior when an RNG is injected  

---

## Installation

Editable install:

```
pip install -e .
```

Install test dependencies:

```
pip install .[test]
```

Run tests:

```
pytest
```

---

## Architecture

### 1. Scheduler
Runs the polling loop.

- loads configuration  
- applies per‑device staggering  
- wraps each poll in a timeout  
- maintains a fixed polling interval  
- supports graceful shutdown  
- exposes `run_once` for integration tests  

### 2. Async SNMP client
Simulates SNMP polling.

- configurable latency  
- jitter spikes  
- partial metric sets  
- malformed responses  
- deterministic behavior with injected RNG  

### 3. Rate calculator
Computes per‑second rates.

- tracks previous values per `(host, oid)`  
- handles wraparound  
- enforces monotonicity  
- supports GAUGE metrics  
- uses LRU eviction to bound memory  

### 4. Reporter
Receives:

- initial values  
- computed rates  
- polling errors  
- timeout errors  

### 5. Configuration
`config.json` defines:

- polling interval  
- devices  
- SNMP community  
- OIDs and SNMP types  

### 6. Tests
Covers:

- counter initialization  
- monotonic increments  
- wraparound  
- gauge behavior  
- LRU eviction  
- deterministic RNG behavior  
- jitter and partial‑set handling  
- scheduler concurrency  
- staggering  
- timeout handling  
- integration with simulated SNMP failures  
- chaos tests for scheduler, SNMP client, and rate calculator  

---

## Directory layout

```
telemetryd/
│
├── config.json
├── main.py
├── pyproject.toml
├── README.md
│
├── src/
│   └── telemetryd/
│       ├── __init__.py
│       ├── metrics.py
│       ├── scheduler.py
│       └── snmp.py
│
└── tests/
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

```
python main.py
```

The scheduler:

- staggers initial polls  
- polls devices concurrently  
- computes rates  
- logs metrics and errors  
- maintains a fixed interval  
