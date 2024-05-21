#!/bin/bash

srun -t 1:00:00 --mem 64G -c 16 singularity run docker://ghcr.io/ebispot/rust_for_codon:1.74 bash -c "export CARGO_HOME=./cargo_home && cargo build --release"
