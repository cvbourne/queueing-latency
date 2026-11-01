"""Capacity planning for keyed consumers (Kafka partitions, Pub/Sub ordering keys).

Ordering is the constraint averages hide. Messages that share a key (or a
partition) are processed one at a time, so the hottest key or partition sets a
ceiling that adding consumers cannot raise. This module answers:

- How many consumers do I need for Y messages/s at a latency target?
- How hot can one key get before something saturates?
- If a key goes 10x hot, how fast does lag build, and how long to drain it?

Units match mm1: service time S in ms, rates in messages per ms.

Two ordering modes:

- "partition" (classic Kafka): keys hash to partitions, partitions are assigned
  round-robin to consumers, and each consumer processes its partitions on one
  thread. The consumer is the server; its arrivals are the sum of its partitions.
- "key" (Pub/Sub ordering keys, keyed parallel consumers): keys hash straight to
  consumers, each consumer runs `workers` threads, and two messages with the same
  key never run at once. A key is its own serial server inside the consumer.
"""
from __future__ import annotations

import hashlib
import heapq
import math
import random
from dataclasses import dataclass, field

import mm1


# ---------- load shape ----------

def zipf_weights(n_keys: int, s: float = 1.0) -> list[float]:
    """Share of traffic per key, key 0 hottest. s = 0 is uniform; s ~ 1 is typical skew."""
    if n_keys < 1:
        raise ValueError("need at least one key")
    raw = [1.0 / (i + 1) ** s for i in range(n_keys)]
    total = sum(raw)
    return [r / total for r in raw]


def stable_hash(key: int, buckets: int) -> int:
    """Deterministic key -> bucket, like a partitioner's murmur/md5 hash (not Python's salted hash)."""
    digest = hashlib.md5(f"key-{key}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % buckets


# ---------- plan ----------

@dataclass
class Plan:
    mode: str
    rate: float                      # total arrivals, msg/ms
    S: float                         # mean service time, ms
    consumers: int
    workers: int
    key_rates: list[float]
    key_to_consumer: list[int]
    partition_rates: list[float] = field(default_factory=list)
    consumer_rates: list[float] = field(default_factory=list)

    @property
    def hottest_key_util(self) -> float:
        return max(self.key_rates) * self.S

    @property
    def consumer_utils(self) -> list[float]:
        return [r * self.S / self.workers for r in self.consumer_rates]

    @property
    def hottest_partition_util(self) -> float | None:
        return max(self.partition_rates) * self.S if self.partition_rates else None

    @property
    def bottleneck(self) -> tuple[str, float]:
        """(what saturates first, its utilization)."""
        c_util = max(self.consumer_utils)
        if self.mode == "key" and self.hottest_key_util > c_util:
            return "hottest key", self.hottest_key_util
        return "busiest consumer", c_util

    @property
    def stable(self) -> bool:
        return self.bottleneck[1] < 1

    @property
    def max_rate(self) -> float:
        """Total msg/ms this layout handles with the same key mix before the bottleneck hits 100%."""
        return self.rate / self.bottleneck[1]

    def p99(self, p: float = 0.99) -> float | None:
        """Modeled p-th percentile time in system at the bottleneck, ms (None if unstable).

        partition mode: the busiest consumer is an M/M/1 queue (merged Poisson arrivals), so this is the
        model's answer. key mode: the hottest key's own M/M/1 queue, a lower bound for that key's messages,
        since they also wait for a free worker.
        """
        if not self.stable:
            return None
        if self.mode == "partition":
            return mm1.percentile(self.S, max(self.consumer_utils), p)
        return mm1.percentile(self.S, self.hottest_key_util, p)

    def lag_growth(self) -> float:
        """Messages/ms of backlog the bottleneck adds while overloaded (0 when stable)."""
        name, util = self.bottleneck
        if util < 1:
            return 0.0
        if name == "hottest key":
            return max(self.key_rates) - 1 / self.S
        i = max(range(self.consumers), key=lambda c: self.consumer_rates[c])
        return self.consumer_rates[i] - self.workers / self.S


def plan(
    rate: float,
    S: float,
    weights: list[float],
    consumers: int,
    partitions: int | None = None,
    mode: str = "partition",
    workers: int = 1,
    key_multipliers: dict[int, float] | None = None,
) -> Plan:
    """Lay the key mix onto consumers. key_multipliers scales individual keys' rates (hot-key scenarios)
    on top of the base mix; other keys keep their absolute rates."""
    if mode not in ("partition", "key"):
        raise ValueError("mode must be 'partition' or 'key'")
    if consumers < 1 or workers < 1:
        raise ValueError("consumers and workers must be >= 1")
    key_rates = [rate * w for w in weights]
    for k, m in (key_multipliers or {}).items():
        key_rates[k] *= m
    total = sum(key_rates)

    if mode == "partition":
        if not partitions:
            raise ValueError("partition mode needs a partition count")
        if workers != 1:
            raise ValueError("partition mode models one thread per consumer; use mode='key' for workers > 1")
        key_to_part = [stable_hash(k, partitions) for k in range(len(weights))]
        part_rates = [0.0] * partitions
        for k, r in enumerate(key_rates):
            part_rates[key_to_part[k]] += r
        # Round-robin assignment; consumers beyond the partition count sit idle.
        cons_rates = [0.0] * consumers
        for p, r in enumerate(part_rates):
            cons_rates[p % consumers] += r
        key_to_cons = [key_to_part[k] % consumers for k in range(len(weights))]
        return Plan(mode, total, S, consumers, workers, key_rates, key_to_cons, part_rates, cons_rates)

    key_to_cons = [stable_hash(k, consumers) for k in range(len(weights))]
    cons_rates = [0.0] * consumers
    for k, r in enumerate(key_rates):
        cons_rates[key_to_cons[k]] += r
    return Plan(mode, total, S, consumers, workers, key_rates, key_to_cons, [], cons_rates)


# ---------- the questions ----------

def consumers_needed(
    rate: float,
    S: float,
    weights: list[float],
    p99_target: float,
    partitions: int | None = None,
    mode: str = "partition",
    workers: int = 1,
    max_consumers: int = 1000,
) -> tuple[int | None, str]:
    """Smallest consumer count whose modeled p99 meets the target, or (None, why not).

    The 'why not' is the point: when the hottest partition or key alone breaks the target,
    no consumer count fixes it; the fix is more partitions or splitting the key.
    """
    rho_max = mm1.max_utilization(S, p99_target)
    limit = min(partitions, max_consumers) if mode == "partition" else max_consumers
    for c in range(1, limit + 1):
        pl = plan(rate, S, weights, c, partitions, mode, workers)
        if mode == "key" and pl.hottest_key_util > rho_max:
            return None, (f"hottest key alone runs at {pl.hottest_key_util:.0%} (max {rho_max:.0%} for this target): "
                          "more consumers won't help; split the key or cut service time")
        if max(pl.consumer_utils) <= rho_max:
            return c, "ok"
    if mode == "partition":
        hp = plan(rate, S, weights, partitions, partitions, mode).hottest_partition_util
        return None, (f"even one consumer per partition leaves the hottest partition at {hp:.0%} "
                      f"(max {rho_max:.0%}): add partitions, split the hot key, or cut service time")
    return None, f"not reachable within {max_consumers} consumers"


def hot_key_headroom(
    rate: float,
    S: float,
    weights: list[float],
    consumers: int,
    partitions: int | None = None,
    mode: str = "partition",
    workers: int = 1,
    key: int = 0,
    max_util: float = 1.0,
) -> float:
    """Largest multiplier on one key's rate before the bottleneck reaches max_util (1.0 = unstable)."""
    def util(m: float) -> float:
        return plan(rate, S, weights, consumers, partitions, mode, workers, {key: m}).bottleneck[1]

    if util(1.0) >= max_util:
        return 0.0
    lo, hi = 1.0, 2.0
    while util(hi) < max_util:
        lo, hi = hi, hi * 2
        if hi > 1e9:
            return math.inf
    for _ in range(60):
        mid = (lo + hi) / 2
        lo, hi = (mid, hi) if util(mid) < max_util else (lo, mid)
    return lo


def drain_time(backlog: float, arrival: float, capacity: float) -> float:
    """ms to clear a backlog when capacity (msg/ms) exceeds arrivals. inf if it never drains."""
    spare = capacity - arrival
    return math.inf if spare <= 0 else backlog / spare


# ---------- simulation (checks the formulas) ----------

def simulate_consumer(
    key_rates: dict[int, float],
    S: float,
    workers: int = 1,
    per_key_ordering: bool = False,
    n_messages: int = 50_000,
    seed: int = 7,
) -> dict:
    """Discrete-event sim of one consumer: Poisson arrivals per key, exponential service.

    workers=1, per_key_ordering=False is the partition-mode consumer (FIFO, one thread).
    per_key_ordering=True never runs two messages of the same key at once; a free worker takes the
    oldest message whose key is idle. Returns mean / p99 time in system (ms), the hottest key's p99,
    and utilization.
    """
    rng = random.Random(seed)
    total = sum(key_rates.values())
    keys = list(key_rates)
    cum, acc = [], 0.0
    for k in keys:
        acc += key_rates[k] / total
        cum.append(acc)

    t, arrivals = 0.0, []
    for _ in range(n_messages):
        t += rng.expovariate(total)
        u = rng.random()
        arrivals.append((t, keys[next(i for i, c in enumerate(cum) if c >= u)]))

    queue: list[tuple[float, int]] = []   # waiting (arrival time, key), FIFO
    busy_keys: set[int] = set()
    done: list[tuple[float, int, float]] = []  # (finish time, key, arrival) heap
    free = workers
    sojourn, hot, busy_time, i, now = [], [], 0.0, 0, 0.0
    hot_key = max(key_rates, key=key_rates.get)

    def start_eligible() -> None:
        nonlocal free, busy_time
        j = 0
        while free and j < len(queue):
            arr, k = queue[j]
            if per_key_ordering and k in busy_keys:
                j += 1
                continue
            queue.pop(j)
            s = rng.expovariate(1 / S)
            busy_time += s
            heapq.heappush(done, (now + s, k, arr))
            busy_keys.add(k)
            free -= 1

    while i < len(arrivals) or done:
        if i < len(arrivals) and (not done or arrivals[i][0] <= done[0][0]):
            now, k = arrivals[i]
            queue.append((now, k))
            i += 1
        else:
            now, k, arr = heapq.heappop(done)
            sojourn.append(now - arr)
            if k == hot_key:
                hot.append(now - arr)
            busy_keys.discard(k)
            free += 1
        start_eligible()

    sojourn.sort()
    hot.sort()
    return {
        "hot_key_p99": hot[int(0.99 * (len(hot) - 1))],
        "mean": sum(sojourn) / len(sojourn),
        "p99": sojourn[int(0.99 * (len(sojourn) - 1))],
        "utilization": busy_time / (now * workers),
    }


# ---------- report ----------

def report(pl: Plan, p99_target: float | None = None, hot_x: float | None = None, backlog: float | None = None,
           weights: list[float] | None = None, partitions: int | None = None) -> str:
    name, util = pl.bottleneck
    lines = [
        f"mode                {pl.mode}  ({pl.consumers} consumers x {pl.workers} worker(s))",
        f"arrivals            {pl.rate * 1000:,.0f} msg/s   service {pl.S:g} ms",
        f"hottest key         {max(pl.key_rates) / pl.rate:.1%} of traffic, {pl.hottest_key_util:.0%} of one thread",
    ]
    if pl.hottest_partition_util is not None:
        lines.append(f"hottest partition   {pl.hottest_partition_util:.0%} of one thread")
    lines += [
        f"busiest consumer    {max(pl.consumer_utils):.0%}   (mean {sum(pl.consumer_utils) / pl.consumers:.0%})",
        f"bottleneck          {name} at {util:.0%}",
        f"max throughput      {pl.max_rate * 1000:,.0f} msg/s with this key mix and layout",
    ]
    if pl.stable:
        lines.append(f"p99 at bottleneck   {pl.p99():.1f} ms" + ("  (lower bound)" if pl.mode == "key" else ""))
    else:
        lines.append(f"UNSTABLE            lag grows {pl.lag_growth() * 1000:,.0f} msg/s at the bottleneck")
    if weights is not None:
        args = (pl.rate, pl.S, weights, pl.consumers, partitions, pl.mode, pl.workers)
        h = hot_key_headroom(*args)
        lines.append("hot-key headroom    none: already saturated" if h == 0 else
                     f"hot-key headroom    key 0 can grow {h:.2f}x before saturating")
        if p99_target is not None:
            n, why = consumers_needed(pl.rate, pl.S, weights, p99_target, partitions, pl.mode, pl.workers)
            lines.append(f"consumers for p99 <= {p99_target:g} ms: {n if n else 'none'}" + ("" if n else f"  ({why})"))
        if hot_x:
            hot = plan(pl.rate, pl.S, weights, pl.consumers, partitions, pl.mode, pl.workers, {0: hot_x})
            hn, hu = hot.bottleneck
            lines.append(f"scenario key 0 x{hot_x:g}  bottleneck {hn} at {hu:.0%}"
                         + (f", lag +{hot.lag_growth() * 1000:,.0f} msg/s" if not hot.stable else ""))
    if backlog:
        i = max(range(pl.consumers), key=lambda c: pl.consumer_rates[c])
        cap = pl.workers / pl.S
        t = drain_time(backlog, pl.consumer_rates[i], cap)
        lines.append(f"drain {backlog:,.0f} msgs  " + ("never (no spare capacity)" if math.isinf(t)
                                                       else f"{t / 1000:,.1f} s on the busiest consumer"))
    return "\n".join(lines)
