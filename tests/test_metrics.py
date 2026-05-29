from telemetryd.metrics import RateCalculator, SNMPResponse


def test_initial_poll_returns_none():
    calc = RateCalculator()
    resp = SNMPResponse(oid="1.1.1", name="pages", value=100, snmp_type="COUNTER32")
    rate = calc.calculate_rate("h", resp, delta_time=2.0)
    assert rate is None


def test_normal_rate_calculation():
    calc = RateCalculator()
    resp1 = SNMPResponse(oid="1.1.1", name="pages", value=100, snmp_type="COUNTER32")
    resp2 = SNMPResponse(oid="1.1.1", name="pages", value=150, snmp_type="COUNTER32")
    calc.calculate_rate("h", resp1, delta_time=0.0)
    rate = calc.calculate_rate("h", resp2, delta_time=2.0)
    assert rate == 25.0


def test_counter32_wraparound_recovery():
    calc = RateCalculator()
    resp1 = SNMPResponse(
        oid="1.1.1", name="pages", value=2**32 - 20, snmp_type="COUNTER32"
    )
    resp2 = SNMPResponse(oid="1.1.1", name="pages", value=15, snmp_type="COUNTER32")
    calc.calculate_rate("h", resp1, delta_time=0.0)
    rate = calc.calculate_rate("h", resp2, delta_time=1.0)
    assert rate == 35.0


def test_counter64_wraparound_recovery():
    calc = RateCalculator()
    resp1 = SNMPResponse(
        oid="2.2.2", name="octets", value=2**64 - 100, snmp_type="COUNTER64"
    )
    resp2 = SNMPResponse(oid="2.2.2", name="octets", value=400, snmp_type="COUNTER64")
    calc.calculate_rate("h", resp1, delta_time=0.0)
    rate = calc.calculate_rate("h", resp2, delta_time=10.0)
    assert rate == 50.0


def test_zero_delta():
    calc = RateCalculator()
    resp1 = SNMPResponse(oid="x", name="m", value=500, snmp_type="COUNTER32")
    resp2 = SNMPResponse(oid="x", name="m", value=500, snmp_type="COUNTER32")
    calc.calculate_rate("h", resp1, delta_time=1.0)
    rate = calc.calculate_rate("h", resp2, delta_time=1.0)
    assert rate == 0.0


def test_invalid_delta_time():
    calc = RateCalculator()
    resp1 = SNMPResponse(oid="x", name="m", value=100, snmp_type="COUNTER32")
    resp2 = SNMPResponse(oid="x", name="m", value=200, snmp_type="COUNTER32")
    calc.calculate_rate("h", resp1, delta_time=1.0)
    rate = calc.calculate_rate("h", resp2, delta_time=0.0)
    assert rate == 0.0


def test_multiple_oids_same_host():
    calc = RateCalculator()
    r1 = SNMPResponse(oid="a", name="m1", value=10, snmp_type="COUNTER32")
    r2 = SNMPResponse(oid="b", name="m2", value=20, snmp_type="COUNTER32")
    calc.calculate_rate("h", r1, delta_time=1.0)
    calc.calculate_rate("h", r2, delta_time=1.0)
    r1b = SNMPResponse(oid="a", name="m1", value=30, snmp_type="COUNTER32")
    r2b = SNMPResponse(oid="b", name="m2", value=70, snmp_type="COUNTER32")
    rate1 = calc.calculate_rate("h", r1b, delta_time=2.0)
    rate2 = calc.calculate_rate("h", r2b, delta_time=2.0)
    assert rate1 == 10.0
    assert rate2 == 25.0


def test_multiple_hosts_same_oid():
    calc = RateCalculator()
    r1 = SNMPResponse(oid="x", name="m", value=100, snmp_type="COUNTER32")
    r2 = SNMPResponse(oid="x", name="m", value=100, snmp_type="COUNTER32")
    calc.calculate_rate("h1", r1, delta_time=1.0)
    calc.calculate_rate("h2", r2, delta_time=1.0)
    r1b = SNMPResponse(oid="x", name="m", value=150, snmp_type="COUNTER32")
    r2b = SNMPResponse(oid="x", name="m", value=130, snmp_type="COUNTER32")
    rate1 = calc.calculate_rate("h1", r1b, delta_time=1.0)
    rate2 = calc.calculate_rate("h2", r2b, delta_time=1.0)
    assert rate1 == 50.0
    assert rate2 == 30.0


def test_gauge_returns_raw_value():
    calc = RateCalculator()
    resp1 = SNMPResponse(oid="g1", name="temp", value=42, snmp_type="GAUGE")
    resp2 = SNMPResponse(oid="g1", name="temp", value=55, snmp_type="GAUGE")

    # First poll returns raw value
    assert calc.calculate_rate("h", resp1, delta_time=1.0) == 42.0

    # Second poll returns raw value (no delta)
    assert calc.calculate_rate("h", resp2, delta_time=1.0) == 55.0


def test_negative_delta_non_counter_resets_to_zero():
    calc = RateCalculator()
    resp1 = SNMPResponse(oid="x", name="m", value=500, snmp_type="GAUGE")
    resp2 = SNMPResponse(oid="x", name="m", value=100, snmp_type="GAUGE")

    calc.calculate_rate("h", resp1, delta_time=1.0)
    rate = calc.calculate_rate("h", resp2, delta_time=1.0)

    # Gauges return raw value even if "negative delta"
    assert rate == 100.0


def test_negative_delta_counter_treated_as_wraparound_or_reset():
    calc = RateCalculator()
    resp1 = SNMPResponse(oid="c", name="ctr", value=500, snmp_type="COUNTER32")
    resp2 = SNMPResponse(oid="c", name="ctr", value=100, snmp_type="COUNTER32")

    calc.calculate_rate("h", resp1, delta_time=1.0)
    rate = calc.calculate_rate("h", resp2, delta_time=1.0)

    # Wraparound correction: (100 - 500) + 2^32
    expected = (100 - 500 + 2**32) / 1.0
    assert rate == round(expected, 2)


def test_monotonicity_never_negative():
    calc = RateCalculator()
    resp1 = SNMPResponse(oid="m", name="ctr", value=1000, snmp_type="COUNTER32")
    resp2 = SNMPResponse(oid="m", name="ctr", value=900, snmp_type="COUNTER32")

    calc.calculate_rate("h", resp1, delta_time=1.0)
    rate = calc.calculate_rate("h", resp2, delta_time=1.0)

    # Even if delta is negative, rate must never be negative
    assert rate >= 0.0


def test_history_eviction_lru_behavior():
    calc = RateCalculator()
    calc.MAX_HISTORY = 5  # shrink for test

    # Insert 5 entries
    for i in range(5):
        resp = SNMPResponse(oid=f"oid{i}", name="m", value=i, snmp_type="COUNTER32")
        calc.calculate_rate("h", resp, delta_time=1.0)

    assert len(calc._history) == 5

    # Insert a 6th → oldest must be evicted
    resp6 = SNMPResponse(oid="oid6", name="m", value=123, snmp_type="COUNTER32")
    calc.calculate_rate("h", resp6, delta_time=1.0)

    assert len(calc._history) == 5
    assert ("h", "oid0") not in calc._history  # evicted
    assert ("h", "oid6") in calc._history  # inserted


def test_history_lru_updates_on_access():
    calc = RateCalculator()
    calc.MAX_HISTORY = 3

    # Insert 3 entries
    for i in range(3):
        resp = SNMPResponse(oid=f"oid{i}", name="m", value=i, snmp_type="COUNTER32")
        calc.calculate_rate("h", resp, delta_time=1.0)

    # Access oid0 → becomes most recently used
    resp0b = SNMPResponse(oid="oid0", name="m", value=10, snmp_type="COUNTER32")
    calc.calculate_rate("h", resp0b, delta_time=1.0)

    # Insert new entry → should evict oid1 (oldest)
    resp_new = SNMPResponse(oid="oidX", name="m", value=999, snmp_type="COUNTER32")
    calc.calculate_rate("h", resp_new, delta_time=1.0)

    assert ("h", "oid1") not in calc._history
    assert ("h", "oid0") in calc._history
    assert ("h", "oidX") in calc._history
