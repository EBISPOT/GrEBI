
import json
import os
import sys
import shlex
import time
import glob
from subprocess import Popen, PIPE, STDOUT

def main():

    if len(sys.argv) < 2:
        print("Usage: grebi_rocks2neo.py <grebi_config.json>")
        exit(1)

    config_filename = os.path.abspath(sys.argv[1])
    print(get_time() + " --- Config filename: " + config_filename, flush=True)

    with open(config_filename, 'r') as f:
        config = json.load(f)

    input_merged_gz_filenames = os.path.join(os.environ['GREBI_HPS_TMP'], "03_merge", "merged.jsonl.0*")

    all_files = glob.glob(input_merged_gz_filenames)
    max_file_num = max(list(map(lambda f: int(f.split('.')[-2]), all_files)))
    print(get_time() + " --- Max file num: " + str(max_file_num))

    os.makedirs(os.path.join(os.environ['GREBI_NFS_TMP'], '05_materialize_edges'), exist_ok=True)

    if config['use_slurm'] == True:
        print("Running rocks2neo on slurm (use_slurm = true)")
        slurm_cmd = ' '.join([
            'sbatch',
            '--wait',
            '-o ' + os.path.abspath(os.path.join(os.environ['GREBI_NFS_TMP'], '05_materialize_edges', 'rocks2neo_%a.log')),
            '--array=0-' + str(max_file_num) + '%' + str(config['slurm_max_workers']['extract']),
            '--time=' + config['slurm_max_time']['extract'],
            '--mem=' + config['slurm_max_memory']['extract'],
            './05_materialize_edges/grebi_rocks2neo.slurm.sh',
            config_filename
        ])
        if os.system(slurm_cmd) != 0:
            print("rocks2neo failed")
            exit(1)
        os.system("tail -n +1 " + os.path.abspath(os.path.join(os.environ['GREBI_NFS_TMP'], '05_materialize_edges', '*.log')))
    else:
        for n in range(max_file_num+1):
            print("Running " + str(n) + " of " + str(max_file_num))
            if os.system('SLURM_ARRAY_TASK_ID=' + str(n) + ' ./05_materialize_edges/grebi_rocks2neo.slurm.sh ' + config_filename) != 0:
                print("rocks2neo failed")
                exit(1)


def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()

    




    






