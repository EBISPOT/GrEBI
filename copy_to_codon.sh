#!/bin/bash

rsync -arv --exclude 'target' --exclude '.git' --exclude 'grebi_ui' ./ ebi-codon-slurm-spotbot:/nfs/production/parkinso/spot/grebi/

