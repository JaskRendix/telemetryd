# telemetryd

`telemetryd` is an asynchronous SNMP polling daemon for collecting printer counters and computing rate‑based metrics. It runs as a background process and polls multiple devices concurrently.

---

## Purpose

- Poll SNMP counters from network printers  
- Track monotonic values per device and OID  
- Detect 32‑bit and 64‑bit counter wraparound  
- Compute per‑second rates using time deltas  
- Log metric values and computed rates

---

## Installation

Install in editable mode:

```
pip install -e .
```

Install test dependencies:

```
pip install .[test]
```

---

## Components

### **1. Scheduler**
Controls the main loop.  
Loads configuration, dispatches polling tasks, tracks drift, and maintains a stable interval.

### **2. Async SNMP client**
Fetches metrics from each device.  
Implements simulated latency and counter increments for local testing.

### **3. Rate calculator**
Stores previous values, computes deltas, corrects wraparound, and returns per‑second rates.

### **4. Configuration**
`config.json` defines devices, OIDs, SNMP types, and polling interval.

### **5. Tests**
`pytest` suite covers:
- initial poll behavior  
- normal rate calculation  
- COUNTER32 wraparound  
- COUNTER64 wraparound  

---

## Directory layout

```
telemetryd/
│
├── config.json
├── main.py
├── src/
│   ├── metrics.py
│   ├── scheduler.py
│   └── snmp.py
└── tests/
    └── test_metrics.py
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
