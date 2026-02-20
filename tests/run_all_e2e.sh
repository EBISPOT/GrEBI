#!/bin/bash
#
# Run E2E tests for all test subgraphs.
#
# Usage:
#   ./tests/run_all_e2e.sh
#
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

SUBGRAPHS=(
    test_clique_merge
    test_edge_linking
    test_multi_datasource
    test_type_hierarchy
)

FAILED=()

for sg in "${SUBGRAPHS[@]}"; do
    echo ""
    echo "=============================================="
    echo "Running E2E test: ${sg}"
    echo "=============================================="
    if bash "${SCRIPT_DIR}/run_e2e.sh" "$sg"; then
        echo "${sg}: PASS"
    else
        echo "${sg}: FAIL"
        FAILED+=("$sg")
    fi
done

echo ""
echo "=============================================="
echo "E2E Summary"
echo "=============================================="
echo "Total: ${#SUBGRAPHS[@]}"
echo "Passed: $(( ${#SUBGRAPHS[@]} - ${#FAILED[@]} ))"
echo "Failed: ${#FAILED[@]}"

if [ ${#FAILED[@]} -gt 0 ]; then
    echo ""
    echo "Failed tests:"
    for f in "${FAILED[@]}"; do
        echo "  - $f"
    done
    exit 1
fi
