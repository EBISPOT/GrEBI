#!/bin/bash

rm -f *.jsonl.gz
curl https://data.monarchinitiative.org/monarch-kg/latest/monarch-kg.jsonl.tar.gz | pigz -d | tar xf -

pigz --best monarch-kg_nodes.jsonl.gz
pigz --best monarch-kg_edges.jsonl.gz




