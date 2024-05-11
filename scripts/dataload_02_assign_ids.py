
import json
import os
import glob
import sys
import subprocess
import shlex
import re
import time

def main():

    config_filename = os.path.abspath(os.path.join('./configs/pipeline_configs/', os.environ['GREBI_CONFIG'] + '.json'))

    with open(config_filename, 'r') as f:
        config = json.load(f)

    datasource_files_listing = os.path.abspath( os.path.join(os.environ['GREBI_NFS_TMP'], os.environ['GREBI_CONFIG'], '01_ingest', 'datasource_files.jsonl') )
    datasource_files = open(datasource_files_listing, 'r').read().split('\n')

    ###
    ### 2. Assign IDs to nodes (merging cliques)
    ###
    # 2.1. Build database of equivalence cliques
    if os.environ['GREBI_USE_SLURM'] == "1":
        print("Building equivalence db on slurm (use_slurm = true)")
        slurm_cmd = ' '.join([
            'srun',
            '--time=' + config['slurm_max_time']['build_equiv_groups'],
            '--mem=' + config['slurm_max_memory']['build_equiv_groups'],
            './02_equivalences/grebi_build_equiv_groups.slurm.sh',
            config_filename
        ])
        if os.system(slurm_cmd) != 0:
            print("Failed to build equivalence groups")
            exit(1)
    else:
        print("Building equivalence db locally (use_slurm = false)")
        cmd = ' '.join([
            './02_equivalences/grebi_build_equiv_groups.slurm.sh',
            config_filename
        ])
        print(cmd)
        if os.system(cmd) != 0:
            print("Failed to build equivalence db")
            exit(1)
    os.sync()

    equiv_groups_txt = os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], '02_equivalences', 'groups.txt'))

    # 2.2. Assign IDs using the equivalences db
    if os.environ['GREBI_USE_SLURM'] == "1":
        print("Assigning IDs on slurm (use_slurm = true)")
        slurm_cmd = ' '.join([
            'sbatch',
            '--wait',
            '-o ' + os.path.abspath(os.path.join(os.environ['GREBI_NFS_TMP'], os.environ['GREBI_CONFIG'], '02_equivalences', 'assign_ids_%a.log')),
            '--array=0-' + str(len(datasource_files)-1) + '%' + str(config['slurm_max_workers']['assign_ids']),
            '--time=' + config['slurm_max_time']['assign_ids'],
            '--mem=' + config['slurm_max_memory']['assign_ids'],
            './02_equivalences/grebi_assign_ids_worker.slurm.sh',
            datasource_files_listing,
            equiv_groups_txt
        ])
        res = os.system(slurm_cmd)
        os.system("tail -n +1 " + os.path.abspath(os.path.join(os.environ['GREBI_NFS_TMP'], os.environ['GREBI_CONFIG'], '02_equivalences', '*.log')))
        if res != 0:
            print("Failed to assign IDs")
            exit(1)
    else:
        print("Assigning IDs locally (use_slurm = false)")
        for n in range(len(datasource_files)):
            print("Running " + str(n) + " of " + str(len(datasource_files)))
            cmd = ' '.join([
                'SLURM_ARRAY_TASK_ID=' + str(n),
                './02_equivalences/grebi_assign_ids_worker.slurm.sh',
                datasource_files_listing,
                equiv_groups_txt
            ])
            print(cmd)
            if os.system(cmd) != 0:
                print("Failed to assign IDs")
                exit(1)
    os.sync()

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
   main()






