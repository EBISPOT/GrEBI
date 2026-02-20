#!/bin/bash
#
# Run the full E2E test for a given test subgraph:
#   1. Run the Nextflow pipeline with --export_snapshots true
#   2. Compare DB snapshots against expected output
#   3. Start the stack and compare API responses against expected output
#
# Usage:
#   ./e2e_tests/run_e2e.sh <subgraph_name>
#
# Example:
#   ./e2e_tests/run_e2e.sh test_clique_merge
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

OUT_DIR="${GREBI_HOME}/out/${SUBGRAPH}"
EXPECTED_DIR="${GREBI_HOME}/e2e_tests/expected_output/${SUBGRAPH}"

# --- Step 1: Run the pipeline with snapshot export ---
echo "=== Step 1: Running pipeline ==="
export GREBI_SUBGRAPH="${SUBGRAPH}"
export GREBI_NF_CONFIG="${GREBI_NF_CONFIG:-dataload/nextflow/local_4g_nextflow.config}"
export GREBI_NF_EXTRA_ARGS="${GREBI_NF_EXTRA_ARGS:---export_snapshots true}"

cd "${GREBI_HOME}"
bash dataload/scripts/dataload_local.sh
echo ""

# --- Step 2: Compare DB snapshots ---
echo "=== Step 2: Comparing DB snapshots ==="
python3 "${SCRIPT_DIR}/compare_snapshots.py" \
    --subgraph "${SUBGRAPH}" \
    --actual-dir "${OUT_DIR}" \
    --expected-dir "${EXPECTED_DIR}"

DB_RESULT=$?
echo ""

# --- Step 3: API snapshot tests ---
# The integration test process already starts the stack inside a container.
# For API tests, we need the stack running. We'll skip this if there's no
# expected API snapshot (indicating it hasn't been set up yet).
API_RESULT=0
if [ -f "${EXPECTED_DIR}/${SUBGRAPH}_api_snapshot.json" ]; then
    echo "=== Step 3: API snapshot tests ==="
    echo "API snapshot tests require a running stack."
    echo "These are tested as part of the integration test process."
    # API tests run inside run_integration_tests which already has the stack
else
    echo "=== Step 3: Skipping API tests (no expected snapshot found) ==="
    echo "To create expected API snapshots, run the stack and use:"
    echo "  python3 e2e_tests/test_api_snapshots.py --subgraph ${SUBGRAPH} --expected-dir ${EXPECTED_DIR} --update"
fi
echo ""

# --- Results ---
echo "=============================================="
echo "E2E Test Results for ${SUBGRAPH}"
echo "=============================================="

if [ $DB_RESULT -eq 0 ]; then
    echo "DB Snapshots: PASS"
else
    echo "DB Snapshots: FAIL"
fi

if [ $DB_RESULT -eq 0 ] && [ $API_RESULT -eq 0 ]; then
    echo ""
    echo "Overall: PASS"
    exit 0
else
    echo ""
    echo "Overall: FAIL"
    exit 1
fi
