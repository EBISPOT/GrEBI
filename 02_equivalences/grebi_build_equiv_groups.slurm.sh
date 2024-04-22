#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 <dir to search for *.equivalences.tsv> <out groups.txt>"
    exit 1
fi

DIR_TO_SEARCH=$1
OUT_GROUPS_TXT=$2


mkdir 
rm -f $OUT_GROUPS_TXT
mkdir -p $(dirname $OUT_GROUPS_TXT)

FILES=$(find $DIR_TO_SEARCH -name '*.equivalences.tsv')

cat $FILES | ./target/release/grebi_equivalences2groups > $OUT_GROUPS_TXT

