#!/bin/bash

/nfs/ftp/public/databases/spot/kg

srun --time=2:0:0 -c 32 --mem 32g tar --use-compress-program="pigz -k " -cf impc_kg_neo4j.tgz data

srun -p datamover --mem=8g --time 1:0:0 cp impc_kg_neo4j.tgz /nfs/ftp/public/databases/spot/kg/
