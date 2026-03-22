"""
Statistical verification module.
Confirms that the generated frequencies match the requested configuration.
"""

from typing import List

from generator import GeneratorConfig, Subscription


def compute_stats(subscriptions: List[Subscription], config: GeneratorConfig) -> dict:
    total = len(subscriptions)
    if total == 0:
        return {}

    field_counts = {}
    eq_counts = {}

    for sub in subscriptions:
        for sf in sub.fields:
            field_counts[sf.name] = field_counts.get(sf.name, 0) + 1
            if sf.operator == "=":
                eq_counts[sf.name] = eq_counts.get(sf.name, 0) + 1

    stats = {"total_subscriptions": total, "fields": {}}

    for fname, req_freq in config.field_frequencies.items():
        count = field_counts.get(fname, 0)
        actual_freq = count / total

        eq_count = eq_counts.get(fname, 0)
        eq_freq_of_field = eq_count / count if count > 0 else 0.0

        req_eq_freq = config.equality_frequencies.get(fname)

        stats["fields"][fname] = {
            "required_freq": req_freq,
            "actual_freq": round(actual_freq, 4),
            "count": count,
            "eq_count": eq_count,
            "eq_freq_of_field": round(eq_freq_of_field, 4),
            "req_eq_freq": req_eq_freq,
        }

    return stats


def print_stats(stats: dict):
    print("\n" + "=" * 60)
    print("  SUBSCRIPTION STATISTICS VERIFICATION")
    print("=" * 60)
    print(f"  Total subscriptions: {stats['total_subscriptions']}")
    print()

    for fname, s in stats["fields"].items():
        ok_freq = "[OK]" if s["actual_freq"] >= s["required_freq"] - 0.01 else "[FAIL]"
        print(f"  Field: {fname}")
        print(f"    Required frequency:  {s['required_freq'] * 100:.0f}%")
        print(f"    Actual frequency:    {s['actual_freq'] * 100:.1f}%  {ok_freq}")
        print(f"    Count: {s['count']}")

        if s["req_eq_freq"] is not None:
            ok_eq = (
                "[OK]" if s["eq_freq_of_field"] >= s["req_eq_freq"] - 0.01 else "[FAIL]"
            )
            print(f"    Required '=' freq (of field):  {s['req_eq_freq'] * 100:.0f}%")
            print(
                f"    Actual   '=' freq (of field):  {s['eq_freq_of_field'] * 100:.1f}%  {ok_eq}"
            )
        print()
    print("=" * 60)
