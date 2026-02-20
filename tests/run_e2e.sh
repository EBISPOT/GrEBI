#!/bin/bash
#
# Run the full E2E test for a given test subgraph.
#
# Runs the Nextflow pipeline with --export_snapshots true, which causes the
# integration test process to also export DB/API snapshots and compare them
# against expected output in tests/expected_output/<subgraph>/.
#
# Usage:
#   ./tests/run_e2e.sh <subgraph_name>
#
# Example:
#   ./tests/run_e2e.sh test_clique_merge
#
set -Eeuo pipefail

SUBGRAPH="${1:?Usage: $0 <subgraph_name>}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GREBI_HOME="$(dirname "$SCRIPT_DIR")"

echo "=============================================="
echo "GrEBI E2E Test: ${SUBGRAPH}"
echo "=============================================="
echo "GREBI_HOME: ${GREBI_HOME}"
echo ""

export GREBI_SUBGRAPH="${SUBGRAPH}"
export GREBI_NF_CONFIG="${GREBI_NF_CONFIG:-dataload/nextflow/local_4g_nextflow.config}"
export GREBI_NF_EXTRA_ARGS="${GREBI_NF_EXTRA_ARGS:---export_snapshots true}"

cd "${GREBI_HOME}"
bash dataload/scripts/dataload_local.sh
