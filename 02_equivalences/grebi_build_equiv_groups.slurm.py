
import os
import shlex
import time
import json
import sys
import glob

def main():
    if len(sys.argv) < 2:
        print("Usage: grebi_build_equiv_groups.slurm.py <grebi_config.json>")
        exit(1)

    config_filename = os.path.abspath(sys.argv[1])
    with open(config_filename, 'r') as f:
        config = json.load(f)

    equivs = glob.glob(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "01_ingest", "**/*.equivalences.tsv"))
    out_groups_txt = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "02_equivalences", "groups.txt")

    os.makedirs(os.path.dirname(out_groups_txt), exist_ok=True)

    cmd = ' '.join([
        'cat ' + ' '.join(equivs),
        '| ./target/release/grebi_equivalences2groups',
    ] + list(map(lambda x: '--add-group ' + ','.join(x), config['additional_equivalence_groups'])) + [
        '>',
        shlex.quote(out_groups_txt)
    ])

    if os.system(cmd) != 0:
        print("build equiv db failed")
        exit(1)

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
   main()

