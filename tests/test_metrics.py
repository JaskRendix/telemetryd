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
