"""M/M/1 latency calculator for model-serving paths.

Every function takes service time S in milliseconds and arrival rate lam in
requests per millisecond. Utilization rho = lam * S. Time in system for an
M/M/1 queue is exponential with mean S / (1 - rho), which is all you need to
get mean, wait, any percentile, and the replica count for a p99 target.
"""
from __future__ import annotations

import math


def utilization(lam: float, S: float) -> float:
    """rho = arrival rate (req/ms) x service time (ms)."""
    return lam * S


def _check_rho(rho: float) -> None:
    if not 0 <= rho < 1:
        raise ValueError(f"utilization must be in [0, 1), got {rho:.3f}; the queue is unstable")


def mean_time(S: float, rho: float) -> float:
    """Mean time in system (wait + service)."""
    _check_rho(rho)
    return S / (1 - rho)


def mean_wait(S: float, rho: float) -> float:
    """Mean time waiting before service starts."""
    return mean_time(S, rho) - S


def percentile(S: float, rho: float, p: float) -> float:
    """p-th percentile of time in system, p in (0, 1).

    Time in system is exponential with mean m = S / (1 - rho), so the
    p-quantile is -ln(1 - p) * m.
    """
    if not 0 < p < 1:
        raise ValueError(f"p must be in (0, 1), got {p}")
    return -math.log(1 - p) * mean_time(S, rho)


def speedup(lam: float, S_old: float, S_new: float) -> float:
    """Ratio of mean time in system before/after cutting service time at a fixed arrival rate.

    This is why a 2.5x faster model gives more than 2.5x faster responses:
    the cut also lowers utilization, which shrinks the queue.
    """
    return mean_time(S_old, utilization(lam, S_old)) / mean_time(S_new, utilization(lam, S_new))


def max_utilization(S: float, p99_target: float, p: float = 0.99) -> float:
    """Highest rho at which the p-th percentile of time in system is <= target.

    Solve -ln(1 - p) * S / (1 - rho) = target for rho.
    """
    rho = 1 - (-math.log(1 - p) * S) / p99_target
    if rho < 0:
        raise ValueError(f"target {p99_target} ms is below the p{int(p*100)} of a single request with no queue")
    return rho


def replicas(lam: float, S: float, p99_target: float, p: float = 0.99) -> int:
    """Minimum replica count so each replica's p-th percentile is <= target with load spread evenly."""
    rho_max = max_utilization(S, p99_target, p)
    # Each replica sees lam / n; need (lam / n) * S <= rho_max  ->  n >= lam * S / rho_max.
    return max(1, math.ceil(lam * S / rho_max - 1e-12))


def report(lam: float, S: float, p99_target: float | None = None) -> str:
    """Human-readable summary for one serving path."""
    rho = utilization(lam, S)
    lines = [
        f"service time        {S:8.2f} ms",
        f"arrival rate        {lam:8.4f} req/ms  ({lam * 1000:.0f} req/s)",
        f"utilization         {rho:8.1%}",
    ]
    if rho >= 1:
        lines.append("UNSTABLE: arrivals exceed capacity; add replicas or cut service time")
        if p99_target is not None:
            n = replicas(lam, S, p99_target)
            lines.append(f"replicas for p99 <= {p99_target:g} ms: {n}  (max utilization {max_utilization(S, p99_target):.1%})")
        return "\n".join(lines)
    lines += [
        f"mean latency        {mean_time(S, rho):8.2f} ms",
        f"mean wait           {mean_wait(S, rho):8.2f} ms",
        f"p50                 {percentile(S, rho, 0.50):8.2f} ms",
        f"p95                 {percentile(S, rho, 0.95):8.2f} ms",
        f"p99                 {percentile(S, rho, 0.99):8.2f} ms",
    ]
    if p99_target is not None:
        n = replicas(lam, S, p99_target)
        lines.append(f"replicas for p99 <= {p99_target:g} ms: {n}  (max utilization {max_utilization(S, p99_target):.1%})")
    return "\n".join(lines)
