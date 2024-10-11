#!/bin/bash

rm -f medgen.obo medgen.owl medgen.obo.gz medgen.owl.gz

wget https://github.com/monarch-initiative/medgen/releases/download/2024-10-06/medgen.obo
robot convert --input medgen.obo --output medgen.owl

gzip -9 medgen.owl
rm -f medgen.obo


