#!/bin/bash

rsync -arv --exclude 'target' --exclude '.git' --exclude 'grebi_ui' --exclude 'tmp' ./ ebi-codon-slurm-spotbot:/nfs/production/parkinso/spot/grebi/

