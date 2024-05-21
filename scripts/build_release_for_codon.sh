#!/bin/bash

rm -rf target

docker run --rm --user "$(id -u)":"$(id -g)" -v "$PWD":/work -w /work ghcr.io/ebispot/rust_for_codon:1.74 \
    bash -c "cargo build --release"

