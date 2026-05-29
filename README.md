# telemetryd

`telemetryd` is an asynchronous SNMP polling daemon for collecting printer counters and computing rate‑based metrics. It polls multiple devices concurrently and reports raw values and computed rates.

---

## Purpose

- Poll SNMP counters from network printers  
- Track monotonic counters per device and OID  
- Handle COUNTER32 and COUNTER64 wraparound  
- Compute per‑second rates from deltas  
- Emit values and rates through a reporter interface  

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

---

## Components

### **1. Scheduler**
Runs the main loop.  
Loads configuration, dispatches polling tasks, tracks drift, and maintains a stable interval.  
Supports graceful shutdown and single‑iteration execution.

### **2. Async SNMP client**
Simulates SNMP polling.  
Uses an injected RNG for deterministic tests.  
Implements latency, jitter spikes, timeouts, malformed responses, and partial metric sets.

### **3. Rate calculator**
Stores previous values, computes deltas, corrects wraparound, and returns per‑second rates.

### **4. Reporter**
Receives metric values and errors.  
Separates output from scheduler logic.

### **5. Configuration**
`config.json` defines devices, OIDs, SNMP types, and polling interval.

### **6. Tests**
Covers:
- counter initialization  
- monotonic increments  
- COUNTER32 and COUNTER64 wraparound  
- deterministic RNG behavior  
- jitter and partial‑set stress  
- scheduler integration with injected SNMP failures  
- concurrency and timing behavior  

---

## Directory layout

```
telemetryd/
│
├── .github/
├── .gitignore
├── config.json
├── LICENSE
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
    },
    {
      "host": "192.168.1.101",
      "port": 161,
      "community": "public",
      "metrics": [
        {"oid": "1.3.6.1.2.1.43.10.2.1.4.1.1", "type": "COUNTER32", "name": "total_pages"}
      ]
    }
  ]
}
```

---

## Running

Start the daemon:

```
python main.py
```

Run tests:

```
pytest
```
