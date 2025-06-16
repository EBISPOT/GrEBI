#!/bin/bash
set -Eeuo pipefail

MAX_MEM=$1

function get_nodes {
    for f in ./neo_nodes_*
    do
        echo -n "--nodes=$f "
    done
}


function get_edges {
    for f in ./neo_edges_*
    do
        echo -n "--relationships=$f "
    done
}


neo4j-admin database import full \
    $(get_nodes) \
    $(get_edges) \
    --ignore-empty-strings=true \
    --array-delimiter="U+001F" \
    --threads=32 \
    --max-off-heap-memory=$MAX_MEM \
    --verbose \
    --read-buffer-size=256m

sleep 5

export HEAP_SIZE=$MAX_MEM
neo4j start
sleep 20

echo Creating neo4j indexes...

cypher-shell -a neo4j://127.0.0.1:7687 --non-interactive -f /opt/grebi_dataload/06_create_neo_db/cypher/ic_scores_1.cypher
sleep 20
cypher-shell -a neo4j://127.0.0.1:7687 --non-interactive -f /opt/grebi_dataload/06_create_neo_db/cypher/ic_scores_2.cypher
sleep 20
cypher-shell -a neo4j://127.0.0.1:7687 --non-interactive -f /opt/grebi_dataload/06_create_neo_db/cypher/create_indexes.cypher

echo Creating neo4j indexes done

sleep 20
neo4j stop

sleep 20
ls -hl $NEO4J_HOME/run


