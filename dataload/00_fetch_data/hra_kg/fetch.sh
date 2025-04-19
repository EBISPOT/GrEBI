#!/bin/bash

rm -f graph.nt
curl https://cdn.humanatlas.io/digital-objects/collection/hra/v2.2/graph.nt | pigz --best > graph.nt.gz
