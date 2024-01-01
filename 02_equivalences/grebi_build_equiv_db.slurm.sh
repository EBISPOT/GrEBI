#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 <out_rocksdb_path> <dir to search for *.equivalences.tsv>"
    exit 1
fi

OUT_ROCKSDB_PATH=$1
DIR_TO_SEARCH=$2

rm -rf $OUT_ROCKSDB_PATH
mkdir -p $OUT_ROCKSDB_PATH

FILES=$(find $DIR_TO_SEARCH -name '*.equivalences.tsv')

cat $FILES | ./target/release/grebi_equivalences2groups > groups.tsv
cat groups.tsv | ./target/release/grebi_groups2rocks $OUT_ROCKSDB_PATH

