"""
Module for saving publications and subscriptions to text files.
"""

import os
from typing import List

from generator import Publication, Subscription


def save_publications(publications: List[Publication], filepath: str):
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        for pub in publications:
            f.write(str(pub) + "\n")
    print(f"  [OK] Publications saved to: {filepath}  ({len(publications)} records)")


def save_subscriptions(subscriptions: List[Subscription], filepath: str):
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        for sub in subscriptions:
            f.write(str(sub) + "\n")
    print(f"  [OK] Subscriptions saved to: {filepath}  ({len(subscriptions)} records)")
