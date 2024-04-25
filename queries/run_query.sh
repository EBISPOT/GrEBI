#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <query_file.cypher>"
    exit 1
fi

cat $1 | tr '\n' ' ' | jq -Rs '{statements:[{statement:.}]}' | curl -X POST \
    -H 'accept:application/json' \
    -H 'content-type:application/json' \
    --data-binary @- \
    http://localhost:7474/db/neo4j/tx/commit \
    | jq
