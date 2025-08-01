#!/usr/bin/env bash

GREBI_MERGE=$(dirname "$0")/../../../target/release/grebi_merge

mkdir -p input
mkdir -p output

for file in *.jsonl; do
    gzip -c "$file" > "input/${file}.gz"
done


echo === With CatDB first

$GREBI_MERGE CatDB:input/catdb.test.jsonl.gz MoreCats:input/morecats.test.jsonl.gz

echo === With MoreCats first

$GREBI_MERGE MoreCats:input/morecats.test.jsonl.gz CatDB:input/catdb.test.jsonl.gz


