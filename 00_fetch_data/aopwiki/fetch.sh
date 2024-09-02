#!/bin/bash

rm -f AOPWikiRDF.ttl
rm -f AOPWikiRDF-Genes.ttl

curl -L https://raw.githubusercontent.com/marvinm2/AOPWikiRDF/master/data/AOPWikiRDF.ttl | gzip > AOPWikiRDF.ttl.gz
curl -L https://raw.githubusercontent.com/marvinm2/AOPWikiRDF/master/data/AOPWikiRDF-Genes.ttl | gzip > AOPWikiRDF-Genes.ttl.gz



