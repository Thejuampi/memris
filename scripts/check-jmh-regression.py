#!/usr/bin/env python3
import argparse
import json
import math
import sys
from pathlib import Path


def load_baseline(path: Path) -> dict[str, float]:
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise ValueError("baseline must be a JSON object {benchmark: score}")
    baseline: dict[str, float] = {}
    for benchmark, score in raw.items():
        baseline[str(benchmark)] = float(score)
    return baseline


def load_current(path: Path) -> dict[str, float]:
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, list):
        raise ValueError("current JMH result must be a JSON array")
    current: dict[str, float] = {}
    for entry in raw:
        benchmark = entry.get("benchmark")
        metric = entry.get("primaryMetric") or {}
        score = metric.get("score")
        if benchmark is None or score is None:
            continue
        value = float(score)
        if not math.isfinite(value):
            continue
        current[str(benchmark)] = value
    return current


def threshold_for(benchmark: str, flat_threshold: float, embedded_threshold: float) -> float:
    if ".flat_" in benchmark:
        return flat_threshold
    if ".embedded_" in benchmark:
        return embedded_threshold
    return max(flat_threshold, embedded_threshold)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fail when JMH throughput regresses beyond threshold.")
    parser.add_argument("--baseline", required=True, help="Path to baseline JSON mapping benchmark->score")
    parser.add_argument("--current", required=True, help="Path to JMH JSON results")
    parser.add_argument("--flat-threshold", type=float, default=0.10, help="Allowed flat throughput regression")
    parser.add_argument("--embedded-threshold", type=float, default=0.15, help="Allowed embedded regression")
    args = parser.parse_args()

    baseline = load_baseline(Path(args.baseline))
    current = load_current(Path(args.current))

    failures: list[str] = []
    for benchmark, baseline_score in baseline.items():
        if benchmark not in current:
            failures.append(f"missing benchmark result: {benchmark}")
            continue
        current_score = current[benchmark]
        if baseline_score <= 0:
            failures.append(f"invalid baseline score for {benchmark}: {baseline_score}")
            continue
        allowed = threshold_for(benchmark, args.flat_threshold, args.embedded_threshold)
        min_score = baseline_score * (1.0 - allowed)
        if current_score < min_score:
            regression = (baseline_score - current_score) / baseline_score
            failures.append(
                f"{benchmark}: baseline={baseline_score:.3f}, current={current_score:.3f}, "
                f"regression={regression:.2%}, allowed={allowed:.2%}"
            )

    if failures:
        print("JMH regression check failed:")
        for failure in failures:
            print(f" - {failure}")
        return 1

    print("JMH regression check passed.")
    for benchmark, baseline_score in baseline.items():
        current_score = current.get(benchmark, float("nan"))
        print(f" - {benchmark}: baseline={baseline_score:.3f}, current={current_score:.3f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
