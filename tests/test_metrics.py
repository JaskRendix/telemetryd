import pytest

from telemetryd.metrics import RateCalculator, SNMPResponse


def resp(oid, value, snmp_type="COUNTER32", name=None):
    return SNMPResponse(
        oid=oid,
        name=name or oid,
        value=value,
        snmp_type=snmp_type,
    )


def test_initial_poll_returns_none():
    calc = RateCalculator()
    t0 = 1000.0
    assert calc.calculate_rate("h", resp("1.1.1", 100), now=t0) is None


@pytest.mark.parametrize(
    "v1, v2, dt, expected",
    [
        (100, 150, 2.0, 25.0),
        (10, 30, 2.0, 10.0),
        (20, 20, 2.0, 0.0),  # zero delta
    ],
)
def test_normal_rate_calculation(v1, v2, dt, expected):
    calc = RateCalculator()
    t0 = 1000.0
    t1 = t0 + dt

    calc.calculate_rate("h", resp("x", v1), now=t0)
    assert calc.calculate_rate("h", resp("x", v2), now=t1) == expected


def test_invalid_delta_time():
    calc = RateCalculator()
    t = 1000.0

    calc.calculate_rate("h", resp("x", 100), now=t)
    assert calc.calculate_rate("h", resp("x", 200), now=t) == 0.0


@pytest.mark.parametrize(
    "snmp_type, max_val",
    [
        ("COUNTER32", 2**32),
        ("COUNTER64", 2**64),
    ],
)
def test_wraparound(snmp_type, max_val):
    calc = RateCalculator()
    t0 = 1000.0
    t1 = 1001.0

    before = max_val - 20
    after = 15

    calc.calculate_rate("h", resp("oid", before, snmp_type), now=t0)
    rate = calc.calculate_rate("h", resp("oid", after, snmp_type), now=t1)

    expected = (after - before + max_val) / 1.0
    assert rate == round(expected, 2)


def test_negative_delta_non_counter_gauge():
    calc = RateCalculator()
    t0 = 1000.0
    t1 = 1001.0

    calc.calculate_rate("h", resp("g", 500, "GAUGE"), now=t0)
    assert calc.calculate_rate("h", resp("g", 100, "GAUGE"), now=t1) == 100.0


def test_negative_delta_counter_treated_as_wrap_or_reset():
    calc = RateCalculator()
    t0 = 1000.0
    t1 = 1001.0

    # Previous value is small → drop as reset
    calc.calculate_rate("h", resp("c", 500), now=t0)
    rate = calc.calculate_rate("h", resp("c", 100), now=t1)

    assert rate is None


def test_monotonicity_never_negative():
    calc = RateCalculator()
    t0 = 1000.0
    t1 = 1001.0

    calc.calculate_rate("h", resp("m", 1000), now=t0)
    rate = calc.calculate_rate("h", resp("m", 900), now=t1)

    # Reset detection returns None, which is acceptable
    assert rate is None or rate >= 0.0


@pytest.mark.parametrize("v1, v2", [(42, 55), (10, 10), (0, 999)])
def test_gauge_returns_raw_value(v1, v2):
    calc = RateCalculator()
    t0 = 1000.0
    t1 = 1001.0

    assert calc.calculate_rate("h", resp("g", v1, "GAUGE"), now=t0) == float(v1)
    assert calc.calculate_rate("h", resp("g", v2, "GAUGE"), now=t1) == float(v2)


def test_multiple_oids_same_host():
    calc = RateCalculator()
    t0 = 1000.0
    t1 = 1002.0

    calc.calculate_rate("h", resp("a", 10), now=t0)
    calc.calculate_rate("h", resp("b", 20), now=t0)

    rate1 = calc.calculate_rate("h", resp("a", 30), now=t1)
    rate2 = calc.calculate_rate("h", resp("b", 70), now=t1)

    assert rate1 == 10.0
    assert rate2 == 25.0


def test_multiple_hosts_same_oid():
    calc = RateCalculator()
    t0 = 1000.0
    t1 = 1001.0

    calc.calculate_rate("h1", resp("x", 100), now=t0)
    calc.calculate_rate("h2", resp("x", 100), now=t0)

    rate1 = calc.calculate_rate("h1", resp("x", 150), now=t1)
    rate2 = calc.calculate_rate("h2", resp("x", 130), now=t1)

    assert rate1 == 50.0
    assert rate2 == 30.0


def test_history_eviction_lru_behavior():
    calc = RateCalculator()
    calc.MAX_OIDS_PER_DEVICE = 5

    t = 1000.0

    # Insert 5 entries
    for i in range(5):
        calc.calculate_rate("h", resp(f"oid{i}", i), now=t)

    assert len(calc._history["h"]) == 5

    # Insert a 6th → evict oldest
    calc.calculate_rate("h", resp("oid6", 123), now=t)

    assert len(calc._history["h"]) == 5
    assert "oid0" not in calc._history["h"]
    assert "oid6" in calc._history["h"]


def test_history_lru_updates_on_access():
    calc = RateCalculator()
    calc.MAX_OIDS_PER_DEVICE = 3

    t0 = 1000.0
    t1 = 1001.0

    # Insert 3 entries
    for i in range(3):
        calc.calculate_rate("h", resp(f"oid{i}", i), now=t0)

    # Access oid0 → becomes MRU
    calc.calculate_rate("h", resp("oid0", 10), now=t1)

    # Insert new entry → should evict oid1
    calc.calculate_rate("h", resp("oidX", 999), now=t1)

    assert "oid1" not in calc._history["h"]
    assert "oid0" in calc._history["h"]
    assert "oidX" in calc._history["h"]


@pytest.mark.parametrize(
    "snmp_type,max_val",
    [
        ("COUNTER32", 2**32),
        ("COUNTER64", 2**64),
    ],
)
def test_high_traffic_wrap_not_reset(snmp_type, max_val):
    calc = RateCalculator()
    t0 = 1000.0
    t1 = 1005.0  # long window

    before = max_val - 100
    after = 200  # small current value but legitimate wrap

    calc.calculate_rate("h", resp("x", before, snmp_type), now=t0)

    # Simulate huge traffic: adjusted_delta ≈ max_val - before + after
    rate = calc.calculate_rate("h", resp("x", after, snmp_type), now=t1)

    expected = (after - before + max_val) / (t1 - t0)
    assert rate == round(expected, 2)


def test_true_reset_detected():
    calc = RateCalculator()
    t0 = 1000.0
    t1 = 1001.0

    calc.calculate_rate("h", resp("r", 3_000_000_000), now=t0)
    rate = calc.calculate_rate("h", resp("r", 10), now=t1)

    assert rate is not None
    assert rate >= 0.0


def test_invalid_timestamp_does_not_overwrite_history():
    calc = RateCalculator()
    t0 = 1000.0

    calc.calculate_rate("h", resp("x", 100), now=t0)

    # Invalid timestamp (same time)
    rate = calc.calculate_rate("h", resp("x", 200), now=t0)
    assert rate == 0.0

    # Now a valid timestamp → must compute correct delta
    rate2 = calc.calculate_rate("h", resp("x", 300), now=t0 + 10)
    assert rate2 == 20.0


def test_counter_alias_normalization():
    calc = RateCalculator()
    t0 = 1000.0
    t1 = 1002.0

    calc.calculate_rate("h", resp("c", 100, "COUNTER"), now=t0)
    rate = calc.calculate_rate("h", resp("c", 200, "COUNTER"), now=t1)

    assert rate == 50.0


def test_physical_rate_sanity_limit():
    calc = RateCalculator()
    calc.MAX_REASONABLE_RATE = 100.0

    t0 = 1000.0
    t1 = 1001.0

    calc.calculate_rate("h", resp("p", 0), now=t0)
    rate = calc.calculate_rate("h", resp("p", 1000), now=t1)

    assert rate is None


def test_mixed_timestamp_sources_do_not_corrupt_history():
    calc = RateCalculator()

    # First sample uses monotonic fallback
    r1 = calc.calculate_rate("h", resp("m", 100))
    assert r1 is None

    # Second sample uses explicit wall-clock time
    r2 = calc.calculate_rate("h", resp("m", 200), current_time=1000.0)
    assert r2 >= 0.0 or r2 is None  # must not crash or poison history


def test_wrap_with_large_delta_time():
    calc = RateCalculator()
    t0 = 1000.0
    t1 = 2000.0  # long window

    max_val = 2**32
    before = max_val - 5
    after = 10

    calc.calculate_rate("h", resp("w", before), now=t0)
    rate = calc.calculate_rate("h", resp("w", after), now=t1)

    expected = (after - before + max_val) / (t1 - t0)
    assert rate == round(expected, 2)


def test_no_reset_when_current_value_large():
    calc = RateCalculator()
    t0 = 1000.0
    t1 = 1001.0

    max_val = 2**32
    calc.calculate_rate("h", resp("nr", max_val - 50), now=t0)

    # current_value is large → not a reset
    rate = calc.calculate_rate("h", resp("nr", 1000), now=t1)

    assert rate is not None


def test_multiple_wraps_sequence():
    calc = RateCalculator()
    t = 1000.0

    max_val = 2**32

    # First wrap
    calc.calculate_rate("h", resp("mw", max_val - 10), now=t)
    r1 = calc.calculate_rate("h", resp("mw", 20), now=t + 1)
    assert r1 == round((20 - (max_val - 10) + max_val) / 1, 2)

    # Second wrap
    r2 = calc.calculate_rate("h", resp("mw", 30), now=t + 2)
    assert r2 == 10.0


def test_history_preserved_on_invalid_sample():
    calc = RateCalculator()
    t0 = 1000.0

    calc.calculate_rate("h", resp("x", 100), now=t0)

    # Invalid sample (same timestamp)
    calc.calculate_rate("h", resp("x", 500), now=t0)

    # Valid sample must compute delta from original 100
    rate = calc.calculate_rate("h", resp("x", 600), now=t0 + 10)
    assert rate == 50.0
