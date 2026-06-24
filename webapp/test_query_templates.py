#!/usr/bin/env python3
"""
GrEBI Integration Tests

Tests the GrEBI stack by executing all query template examples
and verifying that results are returned.
"""

import os
import sys
import time
import requests
import yaml
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
import argparse


class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def main():
    parser = argparse.ArgumentParser(description='GrEBI Integration Tests')
    parser.add_argument(
        '--api-url',
        default='http://localhost:8090',
        help='GrEBI API base URL (default: http://localhost:8090)'
    )
    
    args = parser.parse_args()
    
    if not wait_for_all_services():
        print_colored("\nServices failed to start. Exiting.", Colors.RED)
        sys.exit(1)
    
    print()
    
    passed, total = test_query_templates(args.api_url)
    
    if total == 0:
        print_colored("No tests were run (none matched this graph)", Colors.YELLOW)
        sys.exit(0)
    elif passed == total:
        print_colored("All tests passed! 🎉", Colors.GREEN)
        sys.exit(0)
    else:
        print_colored(f"{total - passed} test(s) failed", Colors.RED)
        sys.exit(1)


def print_colored(text: str, color: str):
    print(f"{color}{text}{Colors.RESET}")


def wait_for_service(url: str, service_name: str, timeout: int = 300, interval: int = 5) -> bool:
    print_colored(f"Waiting for {service_name} at {url}...", Colors.YELLOW)
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code in [200, 404]:
                print_colored(f"✓ {service_name} is ready", Colors.GREEN)
                return True
        except requests.exceptions.RequestException:
            pass
        
        time.sleep(interval)
        elapsed = int(time.time() - start_time)
        print(f"  Still waiting... ({elapsed}s elapsed)")
    
    print_colored(f"✗ {service_name} failed to start within {timeout}s", Colors.RED)
    return False


def dump_service_logs():
    """Dump service log files for debugging when services fail to start."""
    log_files = [
        ("api.log", "GrEBI API stdout"),
        ("api_err.log", "GrEBI API stderr"),
        ("neo4j.log", "Neo4j stdout"),
        ("neo4j_err.log", "Neo4j stderr"),
        ("postgres.log", "PostgreSQL stdout"),
        ("postgres_err.log", "PostgreSQL stderr"),
        ("supervisord.log", "Supervisord"),
        ("supervisord_output.log", "Supervisord output"),
    ]
    print_colored("\n" + "="*80, Colors.YELLOW)
    print_colored("Service Diagnostic Logs", Colors.YELLOW)
    print_colored("="*80, Colors.YELLOW)
    for filename, label in log_files:
        try:
            with open(filename, 'r') as f:
                content = f.read().strip()
            if content:
                # Show last 50 lines for each log
                lines = content.split('\n')
                tail = '\n'.join(lines[-50:])
                print_colored(f"\n--- {label} ({filename}) [last {min(50, len(lines))} of {len(lines)} lines] ---", Colors.YELLOW)
                print(tail)
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"  Could not read {filename}: {e}")
    print_colored("\n" + "="*80, Colors.YELLOW)


def wait_for_all_services(base_url: str = "http://localhost") -> bool:
    services = [
        (f"{base_url}:8090/api/health", "GrEBI API"),
    ]

    # Only check Neo4j Browser when Neo4j server is running (not embedded mode)
    if not os.environ.get("GREBI_NEO4J_EMBEDDED"):
        services.insert(0, (f"{base_url}:7474", "Neo4j Browser"))
    
    all_ready = True
    for url, name in services:
        if not wait_for_service(url, name):
            all_ready = False
    
    if not all_ready:
        dump_service_logs()
    
    return all_ready


def get_available_graphs(api_url: str) -> List[str]:
    try:
        response = requests.get(f"{api_url}/api/v1/graphs", timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print_colored(f"Warning: Could not get graphs from API: {e}", Colors.YELLOW)
        return []


def load_query_templates(templates_dir: Path) -> List[Dict[str, Any]]:
    templates = []
    
    for template_file in sorted(templates_dir.rglob("*.yaml")):
        if template_file.name.startswith("_"):
            continue
            
        try:
            with open(template_file, 'r') as f:
                template = yaml.safe_load(f)
                if template and 'examples' in template:
                    template['_file'] = str(template_file.relative_to(templates_dir))
                    template['_query_id'] = template_file.stem
                    templates.append(template)
        except Exception as e:
            print_colored(f"Warning: Failed to load {template_file}: {e}", Colors.YELLOW)
    
    return templates


def execute_query(
    api_url: str,
    query_id: str,
    params: Dict[str, Any],
    graph: str
) -> Optional[Dict[str, Any]]:
    url = f"{api_url}/api/v1/graphs/{graph}/query/{query_id}"
    
    query_params = {}
    for key, value in params.items():
        query_params[key] = str(value)
    
    try:
        response = requests.get(url, params=query_params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print_colored(f"  Error executing query: {e}", Colors.RED)
        if hasattr(e, 'response') and e.response is not None:
            print_colored(f"  Response: {e.response.text}", Colors.RED)
        return None


def test_query_templates(
    api_url: str = "http://localhost:8090"
) -> tuple[int, int]:
    templates_dir = Path(os.environ.get("GREBI_QUERY_TEMPLATES_PATH", "/opt/query_templates"))
    
    print_colored("\n" + "="*80, Colors.BOLD)
    print_colored("GrEBI Integration Tests", Colors.BOLD)
    print_colored("="*80 + "\n", Colors.BOLD)
    
    available_graphs = get_available_graphs(api_url)
    if not available_graphs:
        print_colored("No graphs available from backend!", Colors.RED)
        return 0, 0
    
    print_colored(f"Available graphs: {', '.join(available_graphs)}\n", Colors.BLUE)
    
    templates = load_query_templates(templates_dir)
    
    if not templates:
        print_colored("No query templates found!", Colors.RED)
        return 0, 0
    
    print_colored(f"Found {len(templates)} query templates\n", Colors.BLUE)
    
    passed = 0
    total = 0
    
    for template in templates:
        query_id = template['_query_id']
        title = template.get('title', query_id)
        examples = template.get('examples', [])
        template_graphs = template.get('graphs', [])
        
        matching_graphs = [sg for sg in template_graphs if sg in available_graphs] if template_graphs else available_graphs
        
        print_colored(f"\n{Colors.BOLD}Testing: {title}{Colors.RESET}", Colors.BLUE)
        print(f"Query ID: {query_id}")
        print(f"File: {template['_file']}")
        print(f"Matching graphs: {', '.join(matching_graphs)}")
        
        if not examples:
            print_colored("  ⚠ No examples defined, skipping", Colors.YELLOW)
            continue
        
        if not matching_graphs:
            print_colored("  ⚠ No matching graphs available, skipping", Colors.YELLOW)
            continue
        
        for graph in matching_graphs:
            print_colored(f"\n  Testing on graph: {graph}", Colors.BLUE)
            
            for i, example in enumerate(examples, 1):
                total += 1
                example_title = example.get('title', f'Example {i}')
                params = example.get('params', {})
                
                print(f"\n    Example {i}/{len(examples)}: {example_title}")
                print(f"    Parameters: {json.dumps(params, indent=4)}")
                
                result = execute_query(api_url, query_id, params, graph)
            
                if result is None:
                    print_colored(f"    ✗ FAILED: Query execution error", Colors.RED)
                    continue
                
                content = result.get('content', [])
                row_count = len(content)
                
                if row_count > 0:
                    passed += 1
                    print_colored(f"    ✓ PASSED: {row_count} rows returned", Colors.GREEN)
                    
                    total_elements = result.get('totalElements', row_count)
                    if total_elements > row_count:
                        print(f"    Total results: {total_elements} (showing page of {row_count})")
                    
                    print(f"\n    First result:")
                    if content:
                        first_row = content[0]
                        for key, value in first_row.items():
                            value_str = str(value)
                            if len(value_str) > 100:
                                value_str = value_str[:100] + "..."
                            print(f"      {key}: {value_str}")
                    
                    if row_count > 1:
                        print(f"\n    ... and {row_count - 1} more rows on this page")
                else:
                    print_colored(f"    ✗ FAILED: 0 rows returned (expected >0)", Colors.RED)
                    print(f"    Response: {json.dumps(result, indent=2)}")
    
    print_colored("\n" + "="*80, Colors.BOLD)
    print_colored("Test Summary", Colors.BOLD)
    print_colored("="*80, Colors.BOLD)
    
    pass_rate = (passed / total * 100) if total > 0 else 0
    
    print(f"\nTotal tests: {total}")
    print_colored(f"Passed: {passed}", Colors.GREEN)
    print_colored(f"Failed: {total - passed}", Colors.RED)
    print(f"Pass rate: {pass_rate:.1f}%\n")
    
    return passed, total


if __name__ == "__main__":
    main()
