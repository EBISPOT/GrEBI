#!/usr/bin/env python3
"""
GrEBI E2E API Tests

Starts the full stack from packaged Neo4j + PostgreSQL tarballs, then tests API
endpoints and compares responses against expected output committed in git.

Can also be used to capture expected output for the first time.
"""

import sys
import json
import time
import argparse
import subprocess
import signal
import os
import urllib.request
import urllib.error
import urllib.parse
import base64
from pathlib import Path
from typing import Optional, Dict, Any, List


class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def print_colored(text: str, color: str):
    print(f"{color}{text}{Colors.RESET}")


def normalise_for_comparison(obj: Any) -> Any:
    """Recursively sort and normalise JSON for stable comparison."""
    if isinstance(obj, dict):
        return {k: normalise_for_comparison(v) for k, v in sorted(obj.items())}
    elif isinstance(obj, list):
        return [normalise_for_comparison(item) for item in obj]
    else:
        return obj


def api_get(base_url: str, path: str, params: Optional[Dict[str, str]] = None) -> Optional[Any]:
    """Make a GET request and return parsed JSON."""
    url = f"{base_url}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print_colored(f"  Error: {e}", Colors.RED)
        return None


def wait_for_service(url: str, name: str, timeout: int = 300) -> bool:
    """Wait for a service to become available."""
    print_colored(f"Waiting for {name} at {url}...", Colors.YELLOW)
    start = time.time()
    while time.time() - start < timeout:
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=5) as resp:
                if resp.status in (200, 404):
                    print_colored(f"  {name} is ready", Colors.GREEN)
                    return True
        except Exception:
            pass
        time.sleep(3)
    print_colored(f"  {name} failed to start within {timeout}s", Colors.RED)
    return False


def collect_api_snapshot(base_url: str, subgraph: str) -> Dict[str, Any]:
    """Collect a deterministic snapshot of key API endpoint responses."""
    snapshot = {}

    # Health check
    snapshot["health"] = api_get(base_url, "/api/health")

    # Subgraphs list
    snapshot["subgraphs"] = api_get(base_url, "/api/v1/graphs")

    # Subgraph metadata
    snapshot["subgraph_metadata"] = api_get(base_url, f"/api/v1/graphs/{subgraph}")

    # Nodes search (first page)
    nodes_resp = api_get(base_url, f"/api/v1/graphs/{subgraph}/nodes", {"size": "100"})
    if nodes_resp:
        # Sort nodes for determinism
        if "content" in nodes_resp:
            nodes_resp["content"] = sorted(
                nodes_resp["content"],
                key=lambda n: n.get("grebi:nodeId", "")
            )
        snapshot["nodes"] = nodes_resp

    # Search (the current API folds search into the nodes endpoint via ?q=)
    search_resp = api_get(base_url, f"/api/v1/graphs/{subgraph}/nodes", {"q": "*", "size": "100"})
    if search_resp:
        if "content" in search_resp:
            search_resp["content"] = sorted(
                search_resp["content"],
                key=lambda n: n.get("grebi:nodeId", n.get("grebi__nodeId", ""))
            )
        snapshot["search"] = search_resp

    # Individual node details + edges for each node found
    if nodes_resp and "content" in nodes_resp:
        node_details = {}
        for node in nodes_resp["content"]:
            node_id = node.get("grebi:nodeId", "")
            if not node_id:
                continue

            # URL-safe base64 to match the API's Base64.getUrlDecoder()
            encoded_id = base64.urlsafe_b64encode(node_id.encode()).decode()

            detail = api_get(base_url, f"/api/v1/graphs/{subgraph}/nodes/{encoded_id}")
            if detail:
                node_details[node_id] = {"node": detail}

            # Outgoing edges
            out_edges = api_get(
                base_url,
                f"/api/v1/graphs/{subgraph}/nodes/{encoded_id}/outgoing_edges",
                {"size": "100"}
            )
            if out_edges:
                if "content" in out_edges:
                    out_edges["content"] = sorted(
                        out_edges["content"],
                        key=lambda e: json.dumps(e, sort_keys=True)
                    )
                node_details[node_id]["outgoing_edges"] = out_edges

            # Incoming edges
            in_edges = api_get(
                base_url,
                f"/api/v1/graphs/{subgraph}/nodes/{encoded_id}/incoming_edges",
                {"size": "100"}
            )
            if in_edges:
                if "content" in in_edges:
                    in_edges["content"] = sorted(
                        in_edges["content"],
                        key=lambda e: json.dumps(e, sort_keys=True)
                    )
                node_details[node_id]["incoming_edges"] = in_edges

        snapshot["node_details"] = dict(sorted(node_details.items()))

    return snapshot


# Paths whose values change between runs and should be ignored in comparison.
# Use fnmatch-style patterns matched against the dotted JSON path.
IGNORE_PATTERNS = [
    "*.start_time",
    "*.end_time",
    "*.time",
]

def _path_ignored(path: str) -> bool:
    import fnmatch
    return any(fnmatch.fnmatch(path, pat) for pat in IGNORE_PATTERNS)


def compare_snapshots(actual: Dict, expected: Dict, path: str = "") -> List[str]:
    """Recursively compare two JSON structures, returning list of differences."""
    if _path_ignored(path):
        return []

    diffs = []

    actual_norm = normalise_for_comparison(actual)
    expected_norm = normalise_for_comparison(expected)

    if actual_norm != expected_norm:
        if isinstance(actual, dict) and isinstance(expected, dict):
            all_keys = set(list(actual.keys()) + list(expected.keys()))
            for key in sorted(all_keys):
                key_path = f"{path}.{key}" if path else key
                if key not in actual:
                    diffs.append(f"Missing key in actual: {key_path}")
                elif key not in expected:
                    diffs.append(f"Extra key in actual: {key_path}")
                else:
                    diffs.extend(compare_snapshots(actual[key], expected[key], key_path))
        elif isinstance(actual, list) and isinstance(expected, list):
            if len(actual) != len(expected):
                diffs.append(f"{path}: list length differs (actual={len(actual)}, expected={len(expected)})")
            for i in range(min(len(actual), len(expected))):
                diffs.extend(compare_snapshots(actual[i], expected[i], f"{path}[{i}]"))
        else:
            actual_str = json.dumps(actual, sort_keys=True)[:200]
            expected_str = json.dumps(expected, sort_keys=True)[:200]
            diffs.append(f"{path}: {actual_str} != {expected_str}")

    return diffs


def main():
    parser = argparse.ArgumentParser(description='GrEBI E2E API Tests')
    parser.add_argument(
        '--subgraph',
        required=True,
        help='Subgraph name'
    )
    parser.add_argument(
        '--api-url',
        default='http://localhost:8090',
        help='GrEBI API base URL'
    )
    parser.add_argument(
        '--expected-dir',
        required=True,
        help='Directory containing expected API response snapshots'
    )
    parser.add_argument(
        '--update',
        action='store_true',
        help='Update expected output instead of comparing'
    )
    args = parser.parse_args()

    expected_dir = Path(args.expected_dir)
    expected_dir.mkdir(parents=True, exist_ok=True)
    subgraph = args.subgraph
    snapshot_file = expected_dir / f"{subgraph}_api_snapshot.json"

    # Wait for API to be ready
    if not wait_for_service(f"{args.api_url}/api/health", "GrEBI API"):
        print_colored("API not available, aborting", Colors.RED)
        sys.exit(1)

    print_colored(f"\nCollecting API snapshot for subgraph: {subgraph}", Colors.BOLD)
    actual = collect_api_snapshot(args.api_url, subgraph)

    if args.update:
        with open(snapshot_file, 'w') as f:
            json.dump(normalise_for_comparison(actual), f, indent=2, sort_keys=True, ensure_ascii=False)
        print_colored(f"Updated expected output: {snapshot_file}", Colors.GREEN)
        sys.exit(0)

    if not snapshot_file.exists():
        print_colored(f"Expected snapshot not found: {snapshot_file}", Colors.RED)
        print_colored("Run with --update to create it for the first time", Colors.YELLOW)
        # Write the actual for easy diffing
        actual_file = expected_dir / f"{subgraph}_api_snapshot.actual.json"
        with open(actual_file, 'w') as f:
            json.dump(normalise_for_comparison(actual), f, indent=2, sort_keys=True, ensure_ascii=False)
        print(f"Actual output saved to: {actual_file}")
        sys.exit(1)

    with open(snapshot_file) as f:
        expected = json.load(f)

    diffs = compare_snapshots(actual, expected)

    if not diffs:
        print_colored("All API response comparisons passed!", Colors.GREEN)
        sys.exit(0)
    else:
        print_colored(f"\n{len(diffs)} difference(s) found:", Colors.RED)
        for diff in diffs[:50]:
            print(f"  {diff}")
        if len(diffs) > 50:
            print(f"  ... and {len(diffs) - 50} more")

        actual_file = expected_dir / f"{subgraph}_api_snapshot.actual.json"
        with open(actual_file, 'w') as f:
            json.dump(normalise_for_comparison(actual), f, indent=2, sort_keys=True, ensure_ascii=False)
        print(f"\nActual output saved to: {actual_file}")
        print_colored("Run with --update to accept the new output", Colors.YELLOW)
        sys.exit(1)


if __name__ == "__main__":
    main()
