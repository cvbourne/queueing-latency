"""Command line.

  python cli.py --service-ms 5 --rps 160 --p99-target 50          # M/M/1 serving path
  python cli.py keyed --rate 2000 --service-ms 2 --keys 500 --zipf 1.1 \
      --partitions 12 --consumers 6 --p99-target 50 --hot-key-x 5   # keyed consumers
"""
from __future__ import annotations

import argparse
import sys

import keyed
import mm1


def serving(argv: list[str]) -> None:
    ap = argparse.ArgumentParser(description="M/M/1 latency for a model-serving path")
    ap.add_argument("--service-ms", type=float, required=True, help="mean service time per request, ms")
    ap.add_argument("--rps", type=float, required=True, help="arrival rate, requests per second (per replica)")
    ap.add_argument("--p99-target", type=float, default=None, help="p99 latency target, ms (prints replica count)")
    args = ap.parse_args(argv)
    print(mm1.report(args.rps / 1000.0, args.service_ms, args.p99_target))


def keyed_main(argv: list[str]) -> None:
    ap = argparse.ArgumentParser(prog="cli.py keyed", description="capacity plan for keyed consumers")
    ap.add_argument("--rate", type=float, required=True, help="total arrivals, messages per second")
    ap.add_argument("--service-ms", type=float, required=True, help="mean processing time per message, ms")
    ap.add_argument("--keys", type=int, required=True, help="number of distinct keys")
    ap.add_argument("--zipf", type=float, default=1.0, help="key skew: 0 uniform, ~1 typical (default 1.0)")
    ap.add_argument("--mode", choices=["partition", "key"], default="partition")
    ap.add_argument("--partitions", type=int, help="partition count (partition mode)")
    ap.add_argument("--consumers", type=int, required=True)
    ap.add_argument("--workers", type=int, default=1, help="threads per consumer (key mode)")
    ap.add_argument("--p99-target", type=float, help="p99 target, ms (prints consumers needed)")
    ap.add_argument("--hot-key-x", type=float, help="scenario: hottest key's rate times this")
    ap.add_argument("--backlog", type=float, help="messages of backlog to drain")
    a = ap.parse_args(argv)
    w = keyed.zipf_weights(a.keys, a.zipf)
    pl = keyed.plan(a.rate / 1000, a.service_ms, w, a.consumers, a.partitions, a.mode, a.workers)
    print(keyed.report(pl, a.p99_target, a.hot_key_x, a.backlog, w, a.partitions))


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "keyed":
        keyed_main(sys.argv[2:])
    else:
        serving(sys.argv[1:])


if __name__ == "__main__":
    main()
