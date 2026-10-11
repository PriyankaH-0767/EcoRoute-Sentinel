"""
Unit tests for EcoRoute Sentinel: Domain Engine & Risk Mathematics
Verifies deterministic mathematical behavior, CPCB AQI conversions,
wind vector alignment, and mitigation decision logic.
"""

import math
import unittest
import sys
import os

# Add root directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app import (
    clamp,
    sigmoid,
    gauss,
    aqi_to_pm25,
    diurnal,
    congestion,
    transport_efficiency,
    conditions_at,
    factor_scores,
    composite,
    instant_hazard,
    evaluate,
    optimise,
    FLEET,
    Environment,
    Route,
    SOURCE_BEARING_DEG,
)


class TestRiskMaths(unittest.TestCase):

    def test_clamp(self):
        self.assertEqual(clamp(0.5, 0.0, 1.0), 0.5)
        self.assertEqual(clamp(-0.2, 0.0, 1.0), 0.0)
        self.assertEqual(clamp(1.5, 0.0, 1.0), 1.0)

    def test_sigmoid(self):
        self.assertAlmostEqual(sigmoid(0.0), 0.5, places=4)
        self.assertTrue(0.0 < sigmoid(-10.0) < 0.001)
        self.assertTrue(0.999 < sigmoid(10.0) < 1.0)

    def test_cpcb_breakpoint_inversion(self):
        # 0 AQI -> 0 PM2.5
        self.assertEqual(aqi_to_pm25(0), 0.0)
        # 50 AQI -> 30 ug/m3 (Good cutoff)
        self.assertAlmostEqual(aqi_to_pm25(50), 30.0, places=1)
        # 100 AQI -> 60 ug/m3 (Satisfactory cutoff)
        self.assertAlmostEqual(aqi_to_pm25(100), 60.0, places=1)
        # 200 AQI -> 90 ug/m3 (Moderate cutoff)
        self.assertAlmostEqual(aqi_to_pm25(200), 90.0, places=1)
        # 500 AQI -> 380 ug/m3 (Severe upper threshold)
        self.assertAlmostEqual(aqi_to_pm25(500), 380.0, places=1)
        # Monotonicity check
        for a in range(0, 500, 25):
            self.assertLessEqual(aqi_to_pm25(a), aqi_to_pm25(a + 25))

    def test_diurnal_cycle(self):
        # Early morning (07:00 IST) inversion peak should be higher than afternoon (15:00 IST) mixing
        peak_morning = diurnal(7.0)
        trough_afternoon = diurnal(15.0)
        self.assertGreater(peak_morning, trough_afternoon)
        self.assertGreater(peak_morning, 1.0)
        self.assertLess(trough_afternoon, 1.0)

    def test_plume_directional_alignment(self):
        # Stubble smoke corridor aligns from 315 degrees (North-West)
        env_aligned = Environment(now_min=390, aqi=300, wind_speed=12.0, wind_from=315, burn=0.8)
        env_opposed = Environment(now_min=390, aqi=300, wind_speed=12.0, wind_from=135, burn=0.8)

        route = FLEET[0]  # Rohini Corridor (high openness)
        cond_aligned = conditions_at(route, env_aligned, 390, "BASE")
        cond_opposed = conditions_at(route, env_opposed, 390, "BASE")

        self.assertGreater(cond_aligned.plume, cond_opposed.plume)
        self.assertAlmostEqual(cond_opposed.plume, 0.0, places=2)
        self.assertGreater(cond_aligned.pm25, cond_opposed.pm25)

    def test_transport_efficiency(self):
        # Smoke stalls in zero wind, dilutes in gale wind, optimal at ~15-20 km/h
        eff_zero = transport_efficiency(0.0)
        eff_mod = transport_efficiency(15.0)
        eff_gale = transport_efficiency(60.0)
        self.assertEqual(eff_zero, 0.0)
        self.assertGreater(eff_mod, eff_gale)

    def test_hazard_monotonicity_with_burn_intensity(self):
        route = FLEET[0]
        h_low = instant_hazard(conditions_at(route, Environment(390, 250, 10.0, 315, 0.1), 390, "BASE"))
        h_high = instant_hazard(conditions_at(route, Environment(390, 250, 10.0, 315, 0.9), 390, "BASE"))
        self.assertGreater(h_high, h_low)

    def test_low_exposure_loop_mitigation(self):
        # Under high plume conditions, the Low-Exposure Loop should yield lower hazard than base road
        route = FLEET[0]
        env_plume = Environment(now_min=400, aqi=350, wind_speed=12.0, wind_from=310, burn=0.85)

        eval_base = evaluate(route, env_plume, route.depart_min, "BASE", 0)
        eval_loop = evaluate(route, env_plume, route.depart_min, "ALT", 0)

        self.assertLess(eval_loop.hazard, eval_base.hazard)
        self.assertLess(eval_loop.dose_ug, eval_base.dose_ug)

    def test_decision_engine_optimisation(self):
        # Severe smoke day should trigger an action (REROUTE, DELAY, or REROUTE+DELAY)
        severe_env = Environment(now_min=390, aqi=380, wind_speed=14.0, wind_from=315, burn=0.90)
        decision = optimise(FLEET[0], severe_env, threshold=65.0, max_delay=45)

        self.assertTrue(decision.triggered)
        self.assertIn(decision.action, ("REROUTE", "DELAY", "REROUTE+DELAY", "ESCALATE"))
        self.assertGreater(decision.reduction_pct, 0.0)

    def test_decision_engine_safe_day(self):
        # Clean monsoon day should NOT trigger rerouting
        clean_env = Environment(now_min=390, aqi=50, wind_speed=12.0, wind_from=200, burn=0.0)
        decision = optimise(FLEET[0], clean_env, threshold=75.0, max_delay=45)

        self.assertFalse(decision.triggered)
        self.assertEqual(decision.action, "NONE")
        self.assertEqual(decision.reduction_pct, 0.0)


if __name__ == "__main__":
    unittest.main()
