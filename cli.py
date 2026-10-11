"""Command line: python cli.py --service-ms 5 --rps 160 --p99-target 50"""
from __future__ import annotations

import argparse

import mm1


def main() -> None:
    ap = argparse.ArgumentParser(description="M/M/1 latency for a model-serving path")
    ap.add_argument("--service-ms", type=float, required=True, help="mean service time per request, ms")
    ap.add_argument("--rps", type=float, required=True, help="arrival rate, requests per second (per replica)")
    ap.add_argument("--p99-target", type=float, default=None, help="p99 latency target, ms (prints replica count)")
    args = ap.parse_args()
    print(mm1.report(args.rps / 1000.0, args.service_ms, args.p99_target))


if __name__ == "__main__":
    main()
