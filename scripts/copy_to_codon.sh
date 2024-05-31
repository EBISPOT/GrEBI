#!/bin/bash

rsync -arv --exclude 'target' --exclude '.git' --exclude 'grebi_ui' --exclude 'tmp' --exclude 'work' ./ ebi-codon-slurm-spotbot:/nfs/production/parkinso/spot/grebi/

