#!/usr/bin/env python3
"""
GrEBI E2E Snapshot Comparison

Compares exported DB snapshots against expected output committed in git.
Exits with code 0 if all snapshots match, 1 if any differ.
"""

import sys
import json
import argparse
from pathlib import Path


class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def print_colored(text: str, color: str):
    print(f"{color}{text}{Colors.RESET}")


def normalise_json_line(line: str) -> str:
    """Parse and re-serialise a JSON line with sorted keys for stable comparison."""
    line = line.strip()
    if not line:
        return ""
    try:
        obj = json.loads(line)
        return json.dumps(obj, sort_keys=True, ensure_ascii=False)
    except json.JSONDecodeError:
        return line


def compare_snapshot_file(actual_path: Path, expected_path: Path) -> bool:
    """Compare two snapshot files line by line after JSON normalisation."""
    if not actual_path.exists():
        print_colored(f"  MISSING: {actual_path}", Colors.RED)
        return False
    if not expected_path.exists():
        print_colored(f"  MISSING expected: {expected_path}", Colors.YELLOW)
        print_colored(f"  To create expected output, copy from: {actual_path}", Colors.YELLOW)
        return False

    with open(actual_path) as f:
        actual_lines = [normalise_json_line(l) for l in f if l.strip()]
    with open(expected_path) as f:
        expected_lines = [normalise_json_line(l) for l in f if l.strip()]

    if actual_lines == expected_lines:
        return True

    # Show diff details
    actual_set = set(actual_lines)
    expected_set = set(expected_lines)

    missing = expected_set - actual_set
    extra = actual_set - expected_set

    if missing:
        print_colored(f"  Lines in expected but not in actual ({len(missing)}):", Colors.RED)
        for line in sorted(missing)[:10]:
            print(f"    - {line}")
        if len(missing) > 10:
            print(f"    ... and {len(missing) - 10} more")

    if extra:
        print_colored(f"  Lines in actual but not in expected ({len(extra)}):", Colors.RED)
        for line in sorted(extra)[:10]:
            print(f"    + {line}")
        if len(extra) > 10:
            print(f"    ... and {len(extra) - 10} more")

    if len(actual_lines) != len(expected_lines):
        print_colored(f"  Line count: actual={len(actual_lines)}, expected={len(expected_lines)}", Colors.RED)

    return False


def main():
    parser = argparse.ArgumentParser(description='Compare GrEBI DB snapshots against expected output')
    parser.add_argument(
        '--subgraph',
        required=True,
        help='Subgraph name (e.g. test_clique_merge)'
    )
    parser.add_argument(
        '--actual-dir',
        required=True,
        help='Directory containing actual snapshot files from pipeline output'
    )
    parser.add_argument(
        '--expected-dir',
        required=True,
        help='Directory containing expected snapshot files committed in git'
    )
    args = parser.parse_args()

    actual_dir = Path(args.actual_dir)
    expected_dir = Path(args.expected_dir)
    subgraph = args.subgraph

    snapshot_files = [
        f"{subgraph}_snapshot_neo4j_nodes.jsonl",
        f"{subgraph}_snapshot_neo4j_edges.jsonl",
        f"{subgraph}_snapshot_postgres_nodes.jsonl",
        f"{subgraph}_snapshot_postgres_edges.jsonl",
    ]

    print_colored(f"\nComparing snapshots for subgraph: {subgraph}", Colors.BOLD)
    print_colored("=" * 60, Colors.BOLD)
    print(f"Actual dir:   {actual_dir}")
    print(f"Expected dir: {expected_dir}")
    print()

    all_passed = True

    for filename in snapshot_files:
        actual_path = actual_dir / filename
        expected_path = expected_dir / filename

        print_colored(f"\nChecking {filename}...", Colors.BLUE)

        if compare_snapshot_file(actual_path, expected_path):
            print_colored(f"  PASS", Colors.GREEN)
        else:
            print_colored(f"  FAIL", Colors.RED)
            all_passed = False

    print()
    print_colored("=" * 60, Colors.BOLD)

    if all_passed:
        print_colored("All snapshot comparisons passed!", Colors.GREEN)
        sys.exit(0)
    else:
        print_colored("Some snapshot comparisons failed!", Colors.RED)
        print()
        print_colored("To update expected output, run:", Colors.YELLOW)
        print(f"  cp {actual_dir}/{subgraph}_snapshot_*.jsonl {expected_dir}/")
        sys.exit(1)


if __name__ == "__main__":
    main()
