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
    # owlmake-ubergraph path: builds a tiny ubergraph with `om ubergraph` and
    # ingests redundant/non-redundant as separate datasources. Before this can
    # pass in CI, generate + commit its expected output once (needs a runner with
    # a high open-files ulimit for neo4j; see tests/expected_output/README.md):
    #   GREBI_SUBGRAPHS=test_ubergraph GREBI_NF_EXTRA_ARGS="--export_snapshots true" \
    #     bash dataload/scripts/dataload_local.sh
    #   cp out/test_ubergraph/test_ubergraph_snapshot_*.jsonl tests/expected_output/test_ubergraph/
    test_ubergraph
    # PDBe SIFTS trial: ingests a small real subset of PDB->UniProt and PDB->EC
    # mappings (tests/data/test_pdbe/). Left out of the active list until its
    # expected output is generated + committed once on a Docker-capable runner:
    #   GREBI_SUBGRAPHS=test_pdbe GREBI_NF_EXTRA_ARGS="--export_snapshots true" \
    #     bash dataload/scripts/dataload_local.sh
    #   mkdir -p tests/expected_output/test_pdbe
    #   cp out/test_pdbe/test_pdbe_snapshot_*.jsonl tests/expected_output/test_pdbe/
    # then add `test_pdbe` to this list.
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
