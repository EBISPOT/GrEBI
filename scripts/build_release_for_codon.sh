#!/bin/bash

rm -rf target

docker build -t rust_environment_for_codon - < Dockerfile.build

docker run --rm --user "$(id -u)":"$(id -g)" -v "$PWD":/work -w /work rust_environment_for_codon \
    bash -c "cargo build --release"

