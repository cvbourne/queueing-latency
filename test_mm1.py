import unittest

import mm1 as s


class TestMM1(unittest.TestCase):
    # Standard worked example: 5 ms service time throughout.

    def test_utilization(self):
        self.assertAlmostEqual(s.utilization(0.16, 5), 0.8)

    def test_wait_at_80pct(self):
        # At 80% utilization with 5 ms service time, mean wait is 20 ms and mean latency 25 ms.
        self.assertAlmostEqual(s.mean_wait(5, 0.8), 20.0, places=6)
        self.assertAlmostEqual(s.mean_time(5, 0.8), 25.0, places=6)

    def test_mean_and_p99_at_70pct(self):
        # Mean latency 16.7 ms but the modeled p99 reaches 76.7 ms.
        self.assertAlmostEqual(s.mean_time(5, 0.7), 16.667, places=2)
        self.assertAlmostEqual(s.percentile(5, 0.7, 0.99), 76.75, places=1)

    def test_p99_at_90pct(self):
        # Going from 70% to 90% utilization moves p99 from ~76.7 ms to ~230 ms.
        self.assertAlmostEqual(s.percentile(5, 0.9, 0.99), 230.3, places=0)

    def test_speedup_5_to_2(self):
        # Cutting service time 5 -> 2 ms at a fixed arrival rate gives ~8.5x on mean latency,
        # far more than the 2.5x model-level speedup, because utilization drops too.
        self.assertAlmostEqual(s.speedup(0.16, 5, 2), 8.5, places=1)

    def test_max_utilization_inverts_percentile(self):
        rho = s.max_utilization(5, 76.75)
        self.assertAlmostEqual(rho, 0.7, places=2)
        self.assertLessEqual(s.percentile(5, rho, 0.99), 76.75 + 1e-6)

    def test_replicas(self):
        # 1 req/ms at 5 ms service needs more than 5 replicas just to be stable;
        # p99 <= 50 ms needs rho <= 0.5395, so 10 replicas.
        self.assertEqual(s.replicas(1.0, 5, 50), 10)
        self.assertLess(s.replicas(1.0, 5, 230), 10)

    def test_replicas_meets_target(self):
        lam, S, target = 1.0, 5, 50
        n = s.replicas(lam, S, target)
        self.assertLessEqual(s.percentile(S, s.utilization(lam / n, S), 0.99), target + 1e-9)
        if n > 1:
            self.assertGreater(s.percentile(S, s.utilization(lam / (n - 1), S), 0.99), target)

    def test_percentile_is_monotone(self):
        self.assertLess(s.percentile(5, 0.5, 0.5), s.percentile(5, 0.5, 0.99))

    def test_unstable_queue_raises(self):
        with self.assertRaises(ValueError):
            s.mean_time(5, 1.0)
        with self.assertRaises(ValueError):
            s.mean_time(5, 1.2)

    def test_report_runs(self):
        out = s.report(0.16, 5, p99_target=50)
        self.assertIn("utilization", out)
        self.assertIn("replicas", out)
        self.assertIn("UNSTABLE", s.report(0.3, 5))


if __name__ == "__main__":
    unittest.main()
