#!/bin/bash

srun -t 1:00:00 --mem 8G singularity run docker://ghcr.io/ebispot/grebi_rust_environment_for_codon:latest bash -c "export CARGO_HOME=./cargo_home && cargo build --release"
