#!/bin/bash
# rm -f *.jsonl.gz
# curl https://data.monarchinitiative.org/monarch-kg/latest/monarch-kg.jsonl.tar.gz | pigz -d | tar xf -

# pigz --best monarch-kg_nodes.jsonl
# pigz --best monarch-kg_edges.jsonl



echo "Processing Monarch KG..."
# The jsonl KG and duckdb download links are broken 

module load pigz 


echo "Downloading monarch kg nodes"
curl -O https://data.monarchinitiative.org/monarch-kg/latest/monarch-kg_nodes.jsonl 
echo "Downloading monarch kg edges"
curl -O https://data.monarchinitiative.org/monarch-kg/latest/monarch-kg_edges.jsonl

echo "Done"

echo "Compressing nodes"
pigz --best monarch-kg_nodes.jsonl


echo "Compressing edges"
pigz --best monarch-kg_edges.jsonl

echo "Done"

# rm -f *.tsv


