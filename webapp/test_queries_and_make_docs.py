#!/usr/bin/env python3
"""
GrEBI Integration Tests & Documentation Generator

Runs query template integration tests (always), and optionally generates
a documentation PDF from the docs/ markdown files.

Usage:
    python3 test_queries_and_make_docs.py [--api-url URL] [--make-docs] [--docs-dir DIR] [--output FILE]
"""

import argparse
import importlib.util
import os
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description="GrEBI integration tests and optional doc generation"
    )
    parser.add_argument(
        "--api-url",
        default="http://localhost:8090",
        help="GrEBI API base URL (default: http://localhost:8090)",
    )
    parser.add_argument(
        "--make-docs",
        action="store_true",
        default=False,
        help="Generate documentation PDF after tests pass",
    )
    parser.add_argument(
        "--docs-dir",
        default=os.environ.get("GREBI_DOCS_PATH", "/opt/docs"),
        help="Path to the docs/ directory",
    )
    parser.add_argument(
        "--output",
        default="grebi-docs.pdf",
        help="Output PDF filename (default: grebi-docs.pdf)",
    )
    args = parser.parse_args()

    # ── Phase 1: Run integration tests (always) ──────────────────────
    print("=" * 80)
    print("Phase 1: Integration Tests")
    print("=" * 80)

    # Import and run test_query_templates from the same directory
    script_dir = Path(__file__).parent
    test_script = script_dir / "test_query_templates.py"

    if not test_script.exists():
        # Fallback: try /opt/ (Docker context)
        test_script = Path("/opt/test_query_templates.py")

    if not test_script.exists():
        print(f"ERROR: test_query_templates.py not found", file=sys.stderr)
        sys.exit(1)

    # Load as module
    spec = importlib.util.spec_from_file_location("test_query_templates", test_script)
    test_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(test_mod)

    # Run tests
    if not test_mod.wait_for_all_services():
        print("\nServices failed to start. Exiting.")
        sys.exit(1)

    passed, total = test_mod.test_query_templates(args.api_url)

    if total == 0:
        print("No tests were run (none matched this graph)")
    elif passed < total:
        print(f"\n{total - passed} test(s) failed")
        # Continue to doc generation even if some tests fail — the docs
        # should still capture whatever output is available.

    test_exit = 0 if (total == 0 or passed == total) else 1

    # ── Phase 2: Generate docs PDF (only if --make-docs) ─────────────
    docs_exit = 0
    if args.make_docs:
        print()
        print("=" * 80)
        print("Phase 2: Documentation PDF Generation")
        print("=" * 80)

        gen_script = script_dir / "generate_docs_pdf.mjs"
        if not gen_script.exists():
            gen_script = Path("/opt/generate_docs_pdf.mjs")

        api2code_src = script_dir / "api2code.mjs"
        if not api2code_src.exists():
            api2code_src = Path("/opt/api2code.mjs")

        if not gen_script.exists():
            print("ERROR: generate_docs_pdf.mjs not found", file=sys.stderr)
            docs_exit = 1
        else:
            result = subprocess.run(
                [
                    "node",
                    str(gen_script),
                    "--api-url",
                    args.api_url,
                    "--docs-dir",
                    args.docs_dir,
                    "--output",
                    args.output,
                ],
            )
            docs_exit = result.returncode
    else:
        print("\nSkipping documentation generation (use --make-docs to enable)")

    # ── Exit ──────────────────────────────────────────────────────────
    if test_exit != 0 or docs_exit != 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
