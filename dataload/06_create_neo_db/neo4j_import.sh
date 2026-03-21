#!/bin/bash
set -Eeuo pipefail

NEO_MEM=$1

export HEAP_SIZE=$NEO_MEM
export JAVA_OPTS="--add-modules jdk.incubator.vector --add-opens=java.base/java.nio=ALL-UNNAMED -Xms$NEO_MEM -Xmx$NEO_MEM"
export NEO4J_dbms_memory_transaction_total_max=0
export NEO4J_dbms_memory_transaction_max=0

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


# Parse memory value to MB and scale read buffer to fit in heap
NEO_MEM_NUM=$(echo "$NEO_MEM" | sed -E 's/[^0-9]//g')
case "$NEO_MEM" in
  *[gG]) NEO_MEM_MB=$(( NEO_MEM_NUM * 1024 )) ;;
  *)     NEO_MEM_MB=$NEO_MEM_NUM ;;
esac
THREADS=$(nproc)
# read-buffer-size * threads must fit well within heap; use 1/4 of heap divided by threads
READ_BUF_MB=$(( NEO_MEM_MB / 4 / THREADS ))
if [ "$READ_BUF_MB" -lt 1 ]; then READ_BUF_MB=1; fi

neo4j-admin database import full \
    $(get_nodes) \
    $(get_edges) \
    --ignore-empty-strings=true \
    --array-delimiter="U+001F" \
    --threads=$THREADS \
    --max-off-heap-memory=$NEO_MEM \
    --verbose \
    --read-buffer-size=${READ_BUF_MB}m

sleep 5

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


