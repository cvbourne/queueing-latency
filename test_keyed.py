import math
import unittest

import keyed as k
import mm1


class TestLoadShape(unittest.TestCase):
    def test_zipf_sums_to_one_and_is_sorted(self):
        w = k.zipf_weights(100, 1.1)
        self.assertAlmostEqual(sum(w), 1.0)
        self.assertEqual(w, sorted(w, reverse=True))

    def test_zipf_zero_is_uniform(self):
        self.assertTrue(all(abs(x - 0.01) < 1e-12 for x in k.zipf_weights(100, 0)))

    def test_hash_is_stable(self):
        self.assertEqual(k.stable_hash(42, 12), k.stable_hash(42, 12))


class TestPlan(unittest.TestCase):
    def test_rates_are_conserved(self):
        w = k.zipf_weights(500, 1.0)
        pl = k.plan(2.0, 2, w, consumers=6, partitions=12)
        self.assertAlmostEqual(sum(pl.partition_rates), 2.0)
        self.assertAlmostEqual(sum(pl.consumer_rates), 2.0)

    def test_average_hides_the_busiest_consumer(self):
        # 2,000 msg/s, 2 ms each, 6 consumers: mean utilization 67%, but key skew puts one consumer past 100%.
        pl = k.plan(2.0, 2, k.zipf_weights(500, 1.0), consumers=6, partitions=12)
        self.assertAlmostEqual(sum(pl.consumer_utils) / 6, 2 / 3)
        self.assertGreater(max(pl.consumer_utils), 1)
        self.assertFalse(pl.stable)
        self.assertGreater(pl.lag_growth(), 0)

    def test_idle_consumers_beyond_partitions(self):
        pl = k.plan(1.0, 1, k.zipf_weights(50, 0), consumers=8, partitions=4)
        self.assertEqual(sum(1 for r in pl.consumer_rates if r == 0), 4)

    def test_partition_mode_p99_is_mm1_at_busiest_consumer(self):
        # 250 msg/s at 2 ms on one consumer is 50% utilization.
        pl = k.plan(0.25, 2, [1.0], consumers=1, partitions=1)
        self.assertAlmostEqual(pl.p99(), mm1.percentile(2, 0.5, 0.99))


class TestQuestions(unittest.TestCase):
    def test_hot_key_caps_throughput_in_key_mode(self):
        # The hottest key is a serial server: at 2 ms it can't exceed 500 msg/s, whatever the consumer count.
        w = k.zipf_weights(500, 1.2)
        n, why = k.consumers_needed(2.0, 2, w, p99_target=50, mode="key", workers=4)
        self.assertIsNone(n)
        self.assertIn("split the key", why)
        many = k.plan(2.0, 2, w, consumers=100, mode="key", workers=4)
        self.assertEqual(many.bottleneck[0], "hottest key")

    def test_consumers_needed_is_minimal(self):
        w = k.zipf_weights(500, 1.0)
        n, _ = k.consumers_needed(2.0, 2, w, p99_target=50, partitions=12)
        self.assertIsNotNone(n)
        rho_max = mm1.max_utilization(2, 50)
        self.assertLessEqual(max(k.plan(2.0, 2, w, n, 12).consumer_utils), rho_max)
        self.assertGreater(max(k.plan(2.0, 2, w, n - 1, 12).consumer_utils), rho_max)

    def test_partitions_cap_the_fix(self):
        n, why = k.consumers_needed(2.0, 2, k.zipf_weights(500, 1.2), p99_target=50, partitions=12)
        self.assertIsNone(n)
        self.assertIn("add partitions", why)

    def test_headroom_lands_on_saturation(self):
        w = k.zipf_weights(200, 0.8)
        args = (1.0, 1, w, 8, 16, "partition", 1)
        m = k.hot_key_headroom(*args)
        self.assertGreater(m, 1)
        self.assertAlmostEqual(k.plan(*args, {0: m}).bottleneck[1], 1.0, places=6)

    def test_drain_time(self):
        self.assertAlmostEqual(k.drain_time(1000, arrival=0.4, capacity=0.5), 10_000)
        self.assertTrue(math.isinf(k.drain_time(1000, arrival=0.5, capacity=0.5)))


class TestSimulationAgrees(unittest.TestCase):
    def test_partition_consumer_matches_mm1(self):
        # One FIFO consumer at 70%: the model's p99 is 76.7 ms for 5 ms service.
        sim = k.simulate_consumer({0: 0.08, 1: 0.06}, S=5, n_messages=60_000, seed=1)
        self.assertAlmostEqual(sim["utilization"], 0.7, delta=0.02)
        self.assertAlmostEqual(sim["p99"], mm1.percentile(5, 0.7, 0.99), delta=0.1 * 76.7)

    def test_key_ordering_bound_holds(self):
        # Hot key at 60% of a thread inside a 4-worker consumer at ~30%: the workers are mostly idle,
        # yet the hot key's messages queue behind each other like an M/M/1 at 60%.
        rates = {0: 0.3, **{i: 0.3 / 30 for i in range(1, 31)}}
        sim = k.simulate_consumer(rates, S=2, workers=4, per_key_ordering=True, n_messages=60_000, seed=2)
        bound = mm1.percentile(2, 0.6, 0.99)
        self.assertAlmostEqual(sim["hot_key_p99"], bound, delta=0.1 * bound)
        self.assertLess(sim["p99"], sim["hot_key_p99"])  # the cold keys pull the overall p99 down
        self.assertLess(sim["utilization"], 0.35)


if __name__ == "__main__":
    unittest.main()
