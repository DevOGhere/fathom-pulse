"""The test of the test: calibration verdicts, including the two negative controls the Day 4 gate demands."""
import random

from pulse.api.calib_math import judge

R = random.Random(7)


def noisy(mu, sd, n):
    return [R.gauss(mu, sd) for _ in range(n)]


def windows(inject: dict[int, float], n_base=600, n_test=100):
    mu = {1: 6.0, 2: 30.0, 3: 50.0}
    sd = {1: 1.0, 2: 8.0, 3: 29.0}      # stage 3 ~ uniform(0,100) from the 100 ms tick: sd ~ 29
    base = {s: noisy(mu[s], sd[s], n_base) for s in mu}
    test = {s: noisy(mu[s] + inject.get(s, 0.0), sd[s], n_test) for s in mu}
    return base, test


def test_real_injection_stage2_is_found_with_right_size():
    b, t = windows({2: 80.0})
    v = judge(b, t, expected=2, truth_ms=80.0)
    assert v["ok"] and v["found"] == 2 and abs(v["size_err_ms"]) <= v["tol_ms"]


def test_real_injection_stage3_is_found_with_right_size():
    b, t = windows({3: 80.0})
    v = judge(b, t, expected=3, truth_ms=80.0)
    assert v["ok"] and v["found"] == 3


def test_negative_control_wrong_expected_stage_must_fail():
    b, t = windows({2: 80.0})
    assert judge(b, t, expected=3, truth_ms=80.0)["ok"] is False      # delay really is in stage 2
    assert judge(b, t, expected=1, truth_ms=80.0)["ok"] is False


def test_negative_control_sham_reports_nothing_found():
    fails = 0
    for _ in range(40):
        b, t = windows({})
        v = judge(b, t, expected=None)
        fails += not v["ok"]
    assert fails <= 2          # 3-sigma on 3 stages: a rare false alarm is allowed, a pass that blames a stage is not


def test_sham_with_a_real_injection_present_is_a_false_alarm():
    b, t = windows({2: 80.0})
    assert judge(b, t, expected=None)["ok"] is False


def test_two_stages_moved_equally_is_not_sure_not_a_guess():
    b, t = windows({2: 80.0, 3: 80.0})
    v = judge(b, t, expected=2, truth_ms=80.0)
    assert v["found"] is None and v["ok"] is False and "not sure" in v["reason"]


def test_wrong_size_fails_even_when_stage_is_right():
    b, t = windows({2: 80.0})
    v = judge(b, t, expected=2, truth_ms=200.0)                      # claimed truth is far from what was measured
    assert v["ok"] is False and "wrong size" in v["reason"]


def test_too_small_to_see_is_not_detected():
    b, t = windows({2: 1.0})
    v = judge(b, t, expected=2, truth_ms=1.0)
    assert v["ok"] is False and "not detected" in v["reason"]


def test_too_few_samples_is_inconclusive_not_a_pass():
    b, t = windows({2: 80.0}, n_test=5)
    v = judge(b, t, expected=2, truth_ms=80.0)
    assert v["status"] == "inconclusive" and v["ok"] is False
