#!/bin/bash

rm -f *.parquet

wget https://ftp.ebi.ac.uk/pub/databases/spot/ols_embeddings/latest/llama-embed-nemotron-8b_pca512_avg.parquet
wget https://ftp.ebi.ac.uk/pub/databases/spot/ols_embeddings/latest/text-embedding-3-small_pca512_avg.parquet
