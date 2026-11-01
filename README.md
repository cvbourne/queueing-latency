# queueing-latency

Queueing arithmetic for two capacity questions, in pure Python with no dependencies:

1. **Serving paths:** "the model is 2.5x faster, so why are responses 8x faster?" An M/M/1 calculator: utilization in, mean / wait / p99 out, plus the replica count that keeps p99 under an SLO.
2. **Keyed consumers:** "we doubled the consumers, so why is lag still growing?" A capacity planner for Kafka partitions and Pub/Sub ordering keys, where the hottest key or partition, not the average, sets the limit.

## Why

Serving latency is a queueing problem before it is a modeling problem. Three things that are not obvious until you work the numbers:

- **Mean latency hides the tail.** At 70% utilization with a 5 ms model, mean latency is 16.7 ms and p99 is 76.7 ms. At 90% it is 50 ms and 230 ms. The mean moved 3x, the tail moved 3x, but nobody's dashboard shows the mean at 50 ms and thinks "p99 is a quarter second."
- **Cutting service time cuts utilization too.** Taking a model from 5 ms to 2 ms at a fixed arrival rate is a 2.5x model-level speedup and an 8.5x system-level speedup, because the queue in front of it shrinks as well.
- **Replica count is a function of the SLO, not of throughput.** Stability needs utilization under 100%. A p99 target of 50 ms with a 5 ms model needs utilization under 54%, which is nearly twice the replicas.

## Model

M/M/1 per replica: Poisson arrivals, exponential service, one server.

| symbol | meaning |
|---|---|
| `S` | mean service time, ms |
| `lam` | arrival rate, requests per ms |
| `rho = lam * S` | utilization |

Time in system is exponential with mean `S / (1 - rho)`, so the p-th percentile is `-ln(1 - p) * S / (1 - rho)`.

## Use

```bash
python cli.py --service-ms 5 --rps 160 --p99-target 50
```

```
service time            5.00 ms
arrival rate          0.1600 req/ms  (160 req/s)
utilization            80.0%
mean latency           25.00 ms
mean wait              20.00 ms
p50                    17.33 ms
p95                    74.89 ms
p99                   115.13 ms
replicas for p99 <= 50 ms: 2  (max utilization 53.9%)
```

From Python:

```python
import mm1
rho = mm1.utilization(lam=0.16, S=5)        # 0.8
mm1.percentile(5, rho, 0.99)                # 115 ms
mm1.speedup(lam=0.16, S_old=5, S_new=2)     # 8.5
mm1.replicas(lam=1.0, S=5, p99_target=50)   # 10
```

## Keyed consumers and hot keys

Ordering is the constraint averages hide. Messages that share a key (or a partition) are processed one at a time, so:

- **The busiest consumer sets the limit, not the average.** Keys hash unevenly and traffic is skewed, so one consumer runs far hotter than the mean.
- **A hot key is a ceiling replicas can't raise.** With per-key ordering, one key's throughput tops out at `1 / S`. At 2 ms per message that's 500 msg/s, whether you run 4 consumers or 400.
- **Partitions cap the fix.** In classic Kafka you can't usefully run more consumers than partitions, so a hot partition means adding partitions or splitting the key.

`keyed.py` models two ordering modes:

| mode | the serial unit | fits |
|---|---|---|
| `partition` | each consumer (one thread over its partitions) | classic Kafka consumer groups |
| `key` | each key, inside consumers with `--workers` threads | Pub/Sub ordering keys, keyed parallel consumers |

Load shape: `--keys` distinct keys with Zipf skew `--zipf` (0 = uniform, ~1 = typical). Keys hash to partitions with a stable hash; partitions go round-robin to consumers.

### Example: the average says 67%

2,000 msg/s, 2 ms each, 500 keys with typical skew, 12 partitions, 6 consumers:

```bash
python cli.py keyed --rate 2000 --service-ms 2 --keys 500 --zipf 1.0 \
    --partitions 12 --consumers 6 --p99-target 50 --hot-key-x 3
```

```
mode                partition  (6 consumers x 1 worker(s))
arrivals            2,000 msg/s   service 2 ms
hottest key         14.7% of traffic, 59% of one thread
hottest partition   75% of one thread
busiest consumer    126%   (mean 67%)
bottleneck          busiest consumer at 126%
max throughput      1,587 msg/s with this key mix and layout
UNSTABLE            lag grows 130 msg/s at the bottleneck
hot-key headroom    none: already saturated
consumers for p99 <= 50 ms: 11
scenario key 0 x3  bottleneck busiest consumer at 244%, lag +719 msg/s
```

Mean utilization is a comfortable 67%. The busiest consumer is at 126%, and its lag grows by 130 messages every second. It takes 11 consumers to meet a 50 ms p99, and at 11 the hottest key can only grow 1.4x before lag builds again.

### Example: more consumers won't help

Per-key ordering with heavier skew: the hottest key is 24% of traffic, so it needs 96% of one thread on its own.

```bash
python cli.py keyed --mode key --rate 2000 --service-ms 2 --keys 500 --zipf 1.2 \
    --consumers 4 --workers 4 --p99-target 50
```

```
busiest consumer    40%   (mean 25%)
bottleneck          hottest key at 96%
p99 at bottleneck   256.2 ms  (lower bound)
consumers for p99 <= 50 ms: none  (hottest key alone runs at 96% (max 82% for this target): more consumers won't help; split the key or cut service time)
```

The consumers are 60% idle and the hot key's messages still wait a quarter second.

### From Python

```python
import keyed
w = keyed.zipf_weights(500, 1.0)
pl = keyed.plan(rate=2.0, S=2, weights=w, consumers=6, partitions=12)   # rate in msg/ms
pl.bottleneck                       # ('busiest consumer', 1.26)
keyed.consumers_needed(2.0, 2, w, p99_target=50, partitions=12)          # (11, 'ok')
keyed.hot_key_headroom(2.0, 2, w, consumers=11, partitions=12)          # ~1.4
keyed.drain_time(backlog=100_000, arrival=1.2, capacity=1.5)           # ms
```

`keyed.simulate_consumer` is a small discrete-event simulation (Poisson arrivals, exponential service, optional per-key ordering) that the tests use to check the formulas: a FIFO consumer at 70% matches the M/M/1 p99, and a hot key at 60% inside a mostly idle 4-worker consumer still shows the M/M/1 tail for its own messages.

## Tests

```bash
python -m unittest -v
```

The tests pin the numbers above and check that `replicas` returns the smallest count that meets the target.

## Limits, and the decisions this is for

M/M/1 assumes exponential service time. Real model-serving paths have service times that are tighter than exponential (a fixed-size forward pass) or heavier (variable-length inputs, batching, GC pauses), so treat the p99 as a direction, not a forecast. Measure the real distribution, then use this to answer the one question it answers well: **at the current arrival rate, how far from the cliff is this path, and does the fix belong in the model or in the replica count?**

The keyed planner shares those assumptions and adds two: arrivals per key are steady (bursts make things worse than modeled), and the busiest consumer drains its own backlog alone (real rebalancing is messier). Use it to answer: **is lag a capacity problem (add consumers), a layout problem (add partitions), or a key problem (split the hot key)?** Those have different owners and different costs, and the averages on a dashboard can't tell them apart.
