#!/usr/bin/env python3
"""
Summarize go-pipeline overhead benchmarks from `go test -bench` output.

Defaults match the current benchmark suite (work sweep + step sweep). The script
prints markdown tables that can be pasted into docs/benchmarks.md.
"""

from __future__ import annotations

import argparse
import re
import sys
from statistics import median


BENCH_OVERHEAD_ITEMS = 4096
WORK_SWEEP = [0, 4, 16, 64, 256, 1024, 4096]
STEP_COUNTS = [1, 2, 4, 8, 16, 32, 64]
COMPOSITE_STAGE_COUNTS = [1, 2, 4, 8, 12]
COMPOSITE_SWEEPS = {
    "OneToMany (expand -> reduce)": "BenchmarkOverheadCompositeOneToMany",
    "Batch (batch -> unbatch)": "BenchmarkOverheadCompositeBatch",
    "BatchChan (batch -> flatten)": "BenchmarkOverheadCompositeBatchChan",
}


def parse_benchmarks(text: str) -> dict[str, list[float]]:
    pattern = re.compile(r"^(Benchmark\S+)\s+\d+\s+([0-9.]+) ns/op", re.M)
    vals: dict[str, list[float]] = {}
    for name, ns in pattern.findall(text):
        vals.setdefault(name, []).append(float(ns))
    if not vals:
        raise ValueError("no benchmark results found")
    return vals


def median_value(vals: dict[str, list[float]], name: str) -> float:
    if name not in vals:
        raise KeyError(f"missing benchmark: {name}")
    values = vals[name]
    if len(values) == 1:
        return values[0]
    return median(values)


def fmt(value: float) -> str:
    if value >= 1000:
        return f"{value:,.1f}"
    return f"{value:.1f}"


def linear_fit(xs: list[int], ys: list[float]) -> tuple[float, float]:
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den = sum((x - mean_x) ** 2 for x in xs)
    slope = num / den
    intercept = mean_y - slope * mean_x
    return slope, intercept


def print_work_sweep(vals: dict[str, list[float]], items: int) -> None:
    print(f"### Work sweep (conc=1, items={items})")
    print(
        "| Work iters | Loop per item (ns) | Channels per item (ns) | "
        "Pipeline per item (ns) | Overhead per item (ns) | Pipeline/Loop | Pipeline/Channels |"
    )
    print("| ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for iters in WORK_SWEEP:
        loop = median_value(vals, f"BenchmarkOverheadWorkSweep/iters={iters}/loop-serial-8")
        channels = median_value(vals, f"BenchmarkOverheadWorkSweep/iters={iters}/channels-8")
        pipe = median_value(vals, f"BenchmarkOverheadWorkSweep/iters={iters}/pipeline-8")
        overhead = pipe - loop
        loop_per_item = loop / items
        channels_per_item = channels / items
        pipe_per_item = pipe / items
        overhead_per_item = overhead / items
        ratio = pipe / loop if loop else 0.0
        ratio_channels = pipe / channels if channels else 0.0
        print(
            f"| {iters} | {fmt(loop_per_item)} | {fmt(channels_per_item)} | {fmt(pipe_per_item)} | "
            f"{fmt(overhead_per_item)} | {ratio:.1f}x | {ratio_channels:.1f}x |"
        )
    print()
    print("Overhead per item stays roughly flat while work per item grows, so the overhead ratio shrinks as work increases.")
    print()


def print_step_sweep(vals: dict[str, list[float]], items: int) -> list[tuple[int, float]]:
    print(f"### Step-count sweep (conc=1, items={items}, work iters=0)")
    print(
        "| Steps | Loop per item (ns) | Channels per item (ns) | Pipeline per item (ns) | "
        "Overhead per item (ns) | Overhead per step (ns) | Pipeline/Channels |"
    )
    print("| ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    overheads: list[tuple[int, float]] = []
    for steps in STEP_COUNTS:
        loop = median_value(vals, f"BenchmarkOverheadStepScaling/steps={steps}/loop-serial-8")
        channels = median_value(vals, f"BenchmarkOverheadStepScaling/steps={steps}/channels-8")
        pipe = median_value(vals, f"BenchmarkOverheadStepScaling/steps={steps}/pipeline-8")
        overhead = pipe - loop
        loop_per_item = loop / items
        channels_per_item = channels / items
        pipe_per_item = pipe / items
        overhead_per_item = overhead / items
        overheads.append((steps, overhead_per_item))
        per_step = overhead_per_item / steps
        ratio_channels = pipe / channels if channels else 0.0
        print(
            f"| {steps} | {fmt(loop_per_item)} | {fmt(channels_per_item)} | {fmt(pipe_per_item)} | "
            f"{fmt(overhead_per_item)} | {fmt(per_step)} | {ratio_channels:.1f}x |"
        )
    print()
    print("Per-step overhead is roughly linear in the number of steps in this run.")
    print()
    return overheads


def print_fit_and_guidance(overheads: list[tuple[int, float]]) -> None:
    xs = [s for s, _ in overheads]
    ys = [v for _, v in overheads]
    slope, intercept = linear_fit(xs, ys)
    print("Linear fit on overhead per item vs steps (conc=1):")
    if abs(intercept) < 10:
        print(
            f"- Overhead per item approx 0 ns + {fmt(slope)} ns * steps "
            f"(fit intercept is ~{fmt(intercept)} ns, within noise)."
        )
    else:
        print(f"- Overhead per item approx {fmt(intercept)} ns + {fmt(slope)} ns * steps")
    print()

    print("### When is it worth it?")
    print("Aim for work per item that is at least 10x the overhead per item (keeps overhead under ~10%).")
    print()
    print("| Steps | Overhead per item (ns) | Work per item for <10% overhead (ns) |")
    print("| ---: | ---: | ---: |")
    for steps, overhead_per_item in overheads:
        threshold = overhead_per_item * 10
        print(f"| {steps} | {fmt(overhead_per_item)} | {fmt(threshold)} |")
    print()

    print_total_overhead_table(overheads, "Steps")


def print_total_overhead_table(overheads: list[tuple[int, float]], label: str) -> None:
    print(f"### Total overhead estimates (conc=1, {label.lower()})")
    print(f"| {label} | Overhead per item (ns) | Total @1M items (s) | Total @1B items (s) |")
    print("| ---: | ---: | ---: | ---: |")
    for count, overhead_per_item in overheads:
        total_1m = overhead_per_item * 1_000_000 / 1e9
        total_1b = overhead_per_item * 1_000_000_000 / 1e9
        print(f"| {count} | {fmt(overhead_per_item)} | {fmt(total_1m)} | {fmt(total_1b)} |")
    print()

def print_composite_sweep(
    vals: dict[str, list[float]],
    items: int,
    label: str,
    bench_prefix: str,
) -> list[tuple[int, float]]:
    names = []
    for stages in COMPOSITE_STAGE_COUNTS:
        names.extend(
            [
                f"{bench_prefix}/stages={stages}/loop-serial-8",
                f"{bench_prefix}/stages={stages}/channels-8",
                f"{bench_prefix}/stages={stages}/pipeline-8",
            ]
        )
    if any(name not in vals for name in names):
        return []

    print(f"### Composite step-count sweep: {label} (conc=1, items={items})")
    print(
        "| Stages | Loop per item (ns) | Channels per item (ns) | Pipeline per item (ns) | "
        "Overhead per item (ns) | Overhead per stage (ns) | Pipeline/Channels |"
    )
    print("| ---: | ---: | ---: | ---: | ---: | ---: | ---: |")

    overheads: list[tuple[int, float]] = []
    for stages in COMPOSITE_STAGE_COUNTS:
        loop = median_value(vals, f"{bench_prefix}/stages={stages}/loop-serial-8")
        channels = median_value(vals, f"{bench_prefix}/stages={stages}/channels-8")
        pipe = median_value(vals, f"{bench_prefix}/stages={stages}/pipeline-8")
        overhead = pipe - loop
        loop_per_item = loop / items
        channels_per_item = channels / items
        pipe_per_item = pipe / items
        overhead_per_item = overhead / items
        overheads.append((stages, overhead_per_item))
        per_stage = overhead_per_item / stages
        ratio_channels = pipe / channels if channels else 0.0
        print(
            f"| {stages} | {fmt(loop_per_item)} | {fmt(channels_per_item)} | {fmt(pipe_per_item)} | "
            f"{fmt(overhead_per_item)} | {fmt(per_stage)} | {ratio_channels:.1f}x |"
        )

    print()
    return overheads


def print_composite_sweeps(vals: dict[str, list[float]], items: int) -> None:
    for label, bench_prefix in COMPOSITE_SWEEPS.items():
        overheads = print_composite_sweep(vals, items, label, bench_prefix)
        if not overheads:
            continue

        slope, intercept = linear_fit([s for s, _ in overheads], [v for _, v in overheads])
        print(f"Linear fit on overhead per item vs stages (conc=1) for {label}:")
        if abs(intercept) < 10:
            print(
                f"- Overhead per item approx 0 ns + {fmt(slope)} ns * stages "
                f"(fit intercept is ~{fmt(intercept)} ns, within noise)."
            )
        else:
            print(f"- Overhead per item approx {fmt(intercept)} ns + {fmt(slope)} ns * stages")
        print()

        print_total_overhead_table(overheads, "Stages")


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize go-pipeline overhead benchmarks.")
    parser.add_argument("bench_file", nargs="?", help="Path to benchmark output (default: stdin).")
    parser.add_argument(
        "--items",
        type=int,
        default=BENCH_OVERHEAD_ITEMS,
        help="Item count used in overhead sweeps.",
    )
    args = parser.parse_args()

    if args.bench_file:
        text = open(args.bench_file, "r", encoding="utf-8").read()
    else:
        text = sys.stdin.read()

    vals = parse_benchmarks(text)

    print_work_sweep(vals, args.items)
    overheads = print_step_sweep(vals, args.items)
    print_fit_and_guidance(overheads)
    print_composite_sweeps(vals, args.items)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
