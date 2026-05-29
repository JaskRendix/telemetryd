# telemetryd

`telemetryd` is an asynchronous SNMP polling daemon for collecting printer counters and computing rate‑based metrics. It polls multiple devices concurrently, handles counter wraparound, and emits both raw values and computed per‑second rates.

---

## Overview

`telemetryd` is designed for environments where printers expose monotonic counters via SNMP. The daemon:

- polls multiple devices concurrently  
- computes deltas and per‑second rates  
- handles COUNTER32/COUNTER64 wraparound  
- applies per‑device staggering to avoid synchronized bursts  
- enforces per‑device timeouts  
- maintains a stable polling interval using drift compensation  

The system is fully asynchronous and testable, with deterministic behavior when an RNG is injected.

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

### **1. Scheduler**
Coordinates the entire polling cycle.

- Loads configuration  
- Applies per‑device random staggering  
- Wraps each poll in a timeout  
- Maintains a stable interval using drift compensation  
- Supports graceful shutdown  
- Can run a single iteration (`run_once`) for integration tests  

### **2. Async SNMP client**
Simulates SNMP polling with:

- configurable latency  
- jitter spikes  
- partial metric sets  
- malformed responses  
- deterministic behavior via injected RNG  

### **3. Rate calculator**
Tracks previous values per `(host, oid)` and computes:

- deltas  
- wraparound‑corrected deltas  
- per‑second rates  

### **4. Reporter**
Receives:

- initial counter values  
- computed rates  
- polling errors  
- timeout errors  

The reporter is intentionally decoupled from the scheduler.

### **5. Configuration**
`config.json` defines:

- polling interval  
- devices  
- SNMP community  
- OIDs and SNMP types  

### **6. Tests**
Covers:

- counter initialization  
- monotonic increments  
- COUNTER32/COUNTER64 wraparound  
- deterministic RNG behavior  
- jitter and partial‑set stress  
- scheduler concurrency  
- staggered startup behavior  
- integration with simulated SNMP failures  

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
    ├── test_daemon.py
    ├── test_daemon_concurrency.py
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

The scheduler will:

- stagger initial polls  
- poll all devices concurrently  
- compute rates  
- log metrics and errors  
- maintain a stable interval  
