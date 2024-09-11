#!/bin/bash
export GREBI_HOME=/nfs/production/parkinso/spot/grebi
export GREBI_TMP=/hps/nobackup/parkinso/spot/grebi/tmp
export GREBI_CONFIG=ebi
export GREBI_IS_EBI=true
export GREBI_TIMESTAMP=$(date +%Y_%m_%d__%H_%M)
export GREBI_MAX_ENTITIES=1000000000
module load nextflow-22.10.1-gcc-11.2.0-ju5saqw
module load python
export PYTHONPATH="/homes/spotbot/.local/lib/python3.6/site-packages:$PYTHONPATH"
cd /hps/nobackup/parkinso/spot/grebi/
export PYTHONUNBUFFERED=true
srun -p datamover --time 1:0:0 --mem 8g bash -c "rm -rf /nfs/public/rw/ontoapps/grebi/staging && mkdir /nfs/public/rw/ontoapps/grebi/staging"
srun --time 3-0:0:0 --mem 8g bash -c "rm -rf work* tmp && python3 ${GREBI_HOME}/scripts/dataload_codon.py"
#srun --time 23:0:0 --mem 8g bash -c "python3 ${GREBI_HOME}/scripts/dataload_codon.py"


