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
    # PDBe SIFTS: ingests a small real subset of PDB->UniProt and PDB->EC
    # mappings (tests/data/test_pdbe/) alongside a minimal set of the UniProt/EC
    # entities they reference, exercising row merge, edge linking and ontology
    # mapping. To regenerate its expected output after intentional changes:
    #   GREBI_SUBGRAPHS=test_pdbe GREBI_NF_EXTRA_ARGS="--export_snapshots true" \
    #     bash dataload/scripts/dataload_local.sh
    #   cp out/test_pdbe_snapshot_{neo4j_nodes,neo4j_edges,postgres_nodes,postgres_edges}.jsonl \
    #     tests/expected_output/test_pdbe/
    test_pdbe
    # GWAS Catalog: a few real association/study rows covering the delimiters the
    # catalog packs into MAPPED_GENE ("A, B", "A - B", "A x B", "A; B") and the
    # multi-trait MAPPED_TRAIT(_URI) columns (issue #12), plus stand-in gene and
    # trait nodes so the variant->gene / ->trait edges form. Regenerate with:
    #   GREBI_SUBGRAPHS=test_gwas GREBI_NF_EXTRA_ARGS="--export_snapshots true" \
    #     bash dataload/scripts/dataload_local.sh
    #   cp out/test_gwas_snapshot_*.jsonl out/test_gwas_api_snapshot.json \
    #     tests/expected_output/test_gwas/
    test_gwas
    # Reactome: a DOID-annotated Disease object and the entities citing it, plus
    # stand-in DOID/MONDO terms, so the disease clique-merges into the ontology
    # node (issue #26). Regenerate expected output as for test_gwas above.
    test_reactome
    # PrimeKG: a few kg.csv rows around psoriasis whose MONDO/HPO/GO/UBERON
    # ids arrive without leading zeros, plus stand-in terms: each PrimeKG node
    # must be the same graph node as the ontology term (issue #58).
    test_primekg
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
