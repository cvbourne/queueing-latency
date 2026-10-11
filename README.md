# queueing-latency

The arithmetic behind "the model is 2.5x faster, so why are responses 8x faster?" An M/M/1 calculator for model-serving paths: utilization in, mean / wait / p99 out, plus the replica count that keeps p99 under an SLO. Pure Python, no dependencies.

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

## Tests

```bash
python -m unittest -v
```

The tests pin the numbers above and check that `replicas` returns the smallest count that meets the target.

## Limits, and the decision this is for

M/M/1 assumes exponential service time. Real model-serving paths have service times that are tighter than exponential (a fixed-size forward pass) or heavier (variable-length inputs, batching, GC pauses), so treat the p99 as a direction, not a forecast. Measure the real distribution, then use this to answer the one question it answers well: **at the current arrival rate, how far from the cliff is this path, and does the fix belong in the model or in the replica count?**
