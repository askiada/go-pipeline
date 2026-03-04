#!/usr/bin/env python3
"""
Summarize go-pipeline benchmark output with channel vs pipeline percent diff.

Usage:
  scripts/bench_overhead_summary.py bench.txt
  go test ./pkg/pipeline -run '^$' -bench Benchmark -benchmem | scripts/bench_overhead_summary.py
"""

from __future__ import annotations

import argparse
import re
import sys
from statistics import median


def parse_benchmarks(text: str) -> dict[str, list[float]]:
    pattern = re.compile(r"^(Benchmark\S+)\s+\d+\s+([0-9.]+) ns/op", re.M)
    values: dict[str, list[float]] = {}
    for name, ns in pattern.findall(text):
        values.setdefault(name, []).append(float(ns))
    if not values:
        raise ValueError("no benchmark results found")
    return values


def split_name(name: str) -> tuple[str, str] | None:
    match = re.match(r"^(Benchmark\S+)/(channels|pipeline)(?:-\d+)?$", name)
    if not match:
        return None
    return match.group(1), match.group(2)


def median_value(values: list[float]) -> float:
    if len(values) == 1:
        return values[0]
    return median(values)


def fmt_ns(value: float) -> str:
    if value >= 1000:
        return f"{value:,.1f}"
    return f"{value:.1f}"


def fmt_percent(value: float) -> str:
    return f"{value:+.1f}%"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarize channel vs pipeline benchmarks with percent diff."
    )
    parser.add_argument("bench_file", nargs="?", help="Path to benchmark output (default: stdin).")
    args = parser.parse_args()

    if args.bench_file:
        with open(args.bench_file, "r", encoding="utf-8") as handle:
            text = handle.read()
    else:
        text = sys.stdin.read()

    raw = parse_benchmarks(text)
    grouped: dict[str, dict[str, float]] = {}

    for name, samples in raw.items():
        split = split_name(name)
        if split is None:
            continue
        base, kind = split
        grouped.setdefault(base, {})[kind] = median_value(samples)

    print("| Benchmark | Channels ns/op | Pipeline ns/op | Diff |")
    print("| --- | ---: | ---: | ---: |")

    missing: list[str] = []
    for base in sorted(grouped):
        row = grouped[base]
        if "channels" not in row or "pipeline" not in row:
            missing.append(base)
            continue

        channels = row["channels"]
        pipeline = row["pipeline"]
        diff = (pipeline - channels) / channels * 100 if channels else 0.0
        name = base
        if name.startswith("Benchmark"):
            name = name[len("Benchmark") :]
        print(f"| {name} | {fmt_ns(channels)} | {fmt_ns(pipeline)} | {fmt_percent(diff)} |")

    if missing:
        print(
            "Missing channel/pipeline pairs for: " + ", ".join(sorted(missing)),
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
