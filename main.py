"""
Main entry point.
Supports single run OR comparative benchmark (1 thread vs N threads).
"""

import argparse
import os
import sys

from generator import GeneratorConfig, PubSubGenerator
from stats import compute_stats, print_stats
from writer import save_publications, save_subscriptions


def run_once(
    config: GeneratorConfig, output_dir: str = "output", show_stats: bool = True
):
    print(f"\n{'-' * 60}")
    print(f"  Running with {config.num_threads} thread(s)")
    print(
        f"  Publications: {config.num_publications}  |  Subscriptions: {config.num_subscriptions}"
    )
    print(f"{'-' * 60}")

    gen = PubSubGenerator(config)
    publications, subscriptions, timing = gen.generate()

    print(f"\n  Generation times:")
    print(f"    Publications:  {timing['pub_time_s']:.4f} s")
    print(f"    Subscriptions: {timing['sub_time_s']:.4f} s")
    print(f"    TOTAL:         {timing['total_time_s']:.4f} s")

    tag = f"t{config.num_threads}"
    pub_path = os.path.join(output_dir, f"publications_{tag}.txt")
    sub_path = os.path.join(output_dir, f"subscriptions_{tag}.txt")

    print()
    save_publications(publications, pub_path)
    save_subscriptions(subscriptions, sub_path)

    if show_stats:
        stats = compute_stats(subscriptions, config)
        print_stats(stats)

    return timing


def benchmark(config: GeneratorConfig, thread_counts, output_dir: str = "output"):
    """Runs generation for multiple thread counts and compares timings."""
    results = {}
    for n_threads in thread_counts:
        cfg = GeneratorConfig(
            num_publications=config.num_publications,
            num_subscriptions=config.num_subscriptions,
            field_frequencies=config.field_frequencies,
            equality_frequencies=config.equality_frequencies,
            value_range=config.value_range,
            drop_range=config.drop_range,
            variation_range=config.variation_range,
            num_threads=n_threads,
        )
        timing = run_once(cfg, output_dir, show_stats=(n_threads == thread_counts[-1]))
        results[n_threads] = timing

    print("\n" + "=" * 60)
    print("  BENCHMARK RESULTS")
    print("=" * 60)
    print(
        f"  {'Threads':<10} {'Pub (s)':<12} {'Sub (s)':<12} {'Total (s)':<12} {'Speedup'}"
    )
    print(
        f"  {'-------':<10} {'-------':<12} {'-------':<12} {'-------':<12} {'-------'}"
    )

    base_total = results[thread_counts[0]]["total_time_s"]
    for n_threads, timing in results.items():
        speedup = (
            base_total / timing["total_time_s"] if timing["total_time_s"] > 0 else 1.0
        )
        print(
            f"  {n_threads:<10} "
            f"{timing['pub_time_s']:<12.4f} "
            f"{timing['sub_time_s']:<12.4f} "
            f"{timing['total_time_s']:<12.4f} "
            f"x{speedup:.2f}"
        )
    print("=" * 60)
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Pub-Sub publications and subscriptions generator"
    )
    parser.add_argument(
        "--mode",
        choices=["single", "benchmark"],
        default="benchmark",
        help="Run mode: single (one config) or benchmark (comparative)",
    )
    parser.add_argument(
        "--publications",
        type=int,
        default=10000,
        help="Number of publications to generate (default: 10000)",
    )
    parser.add_argument(
        "--subscriptions",
        type=int,
        default=10000,
        help="Number of subscriptions to generate (default: 10000)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="Number of threads for single mode (default: 4)",
    )
    parser.add_argument(
        "--benchmark-threads",
        type=str,
        default="1,4",
        help="Comma-separated thread counts for benchmark, e.g. 1,2,4,8 (default: 1,4)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="output",
        help="Output directory (default: output/)",
    )

    args = parser.parse_args()

    config = GeneratorConfig(
        num_publications=args.publications,
        num_subscriptions=args.subscriptions,
        field_frequencies={
            "company": 0.9,
            "value": 0.7,
            "drop": 0.5,
            "variation": 0.6,
            "date": 0.4,
        },
        equality_frequencies={
            "company": 0.7,  # 70% of subscriptions containing "company" will use "="
        },
        value_range=(10.0, 200.0),
        drop_range=(0.0, 50.0),
        variation_range=(0.0, 5.0),
        num_threads=args.threads,
    )

    os.makedirs(args.output_dir, exist_ok=True)

    if args.mode == "single":
        run_once(config, args.output_dir, show_stats=True)
    else:
        thread_counts = [int(x) for x in args.benchmark_threads.split(",")]
        benchmark(config, thread_counts, args.output_dir)


if __name__ == "__main__":
    main()
