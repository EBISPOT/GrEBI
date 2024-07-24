#!/bin/bash

rm -f blazegraph.jnl
wget https://cdn.humanatlas.io/digital-objects/blazegraph.jnl

docker run --entrypoint /data/entrypoint.dockersh -v $(pwd):/data ghcr.io/ebispot/blazegraph-docker:2.1.5 

