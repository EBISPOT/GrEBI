
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

    input_merged_gz_filenames = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "03_merge", "merged.jsonl.0*")

    all_files = glob.glob(input_merged_gz_filenames)
    max_file_num = max(list(map(lambda f: int(f.split('.')[-1]), all_files)))
    print(get_time() + " --- Max file num: " + str(max_file_num))

    os.makedirs(os.path.join(os.environ['GREBI_NFS_TMP'], os.environ['GREBI_CONFIG'], '06_prepare_db_import'), exist_ok=True)

    if os.environ['GREBI_USE_SLURM'] == "1":
        print("Running prepare_db_import on slurm (use_slurm = true)")
        slurm_cmd = ' '.join([
            'sbatch',
            '--wait',
            '-o ' + os.path.abspath(os.path.join(os.environ['GREBI_NFS_TMP'], os.environ['GREBI_CONFIG'], '06_prepare_db_import', 'prepare_db_import_%a.log')),
            '--array=0-' + str(max_file_num) + '%' + str(config['slurm_max_workers']['prepare_db_import']),
            '--time=' + config['slurm_max_time']['prepare_db_import'],
            '--mem=' + config['slurm_max_memory']['prepare_db_import'],
            './06_prepare_db_import/grebi_prepare_db_import.slurm.sh',
            config_filename
        ])
        if os.system(slurm_cmd) != 0:
            print("prepare_db_import failed")
            exit(1)
        os.system("tail -n +1 " + os.path.abspath(os.path.join(os.environ['GREBI_NFS_TMP'], os.environ['GREBI_CONFIG'], '06_prepare_db_import', '*.log')))
    else:
        for n in range(max_file_num+1):
            print("Running " + str(n) + " of " + str(max_file_num))
            if os.system('SLURM_ARRAY_TASK_ID=' + str(n) + ' ./06_prepare_db_import/grebi_prepare_db_import.slurm.sh ' + config_filename) != 0:
                print("prepare_db_import failed")
                exit(1)



def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
   main()





