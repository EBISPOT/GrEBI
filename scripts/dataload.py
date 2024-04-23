
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

    datasources = map(lambda x: json.load(open(x, 'r')), config['datasource_configs'])
    datasource_files = []

    for datasource in datasources:
        if datasource['enabled'] != True:
            print("Skipping disabled datasource: " + datasource['name'])
        else:
            for ingest in datasource['ingests']:
                for g in ingest['ingest_files']:
                    files = glob.glob(g)
                    for file in files:
                        filename = os.path.abspath(file)
                        basename = os.path.splitext(os.path.basename(filename))[0]
                        nodes_jsonl_gz_filename = os.path.abspath( os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], '01_ingest', datasource['name'], basename + '.jsonl.gz' ))
                        equivalences_tsv_filename = os.path.abspath( os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], '01_ingest', datasource['name'], basename  + '.equivalences.tsv'  ))
                        expanded_subjects_jsonl_filename = os.path.abspath( os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], '02_equivalences', datasource['name'], basename + '.expanded.jsonl' ))
                        sorted_expanded_subjects_jsonl_filename = os.path.abspath( os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], '02_equivalences', datasource['name'], basename + '.sorted_expanded.jsonl' ))
                        sorted_expanded_subjects_jsonl_gz_filename = os.path.abspath( os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], '02_equivalences', datasource['name'], basename + '.sorted_expanded.jsonl.gz' ))
                        datasource_files.append(json.dumps({
                            'config': config_filename,
                            'datasource': datasource,
                            'ingest': ingest,
                            'filename': filename,
                            'artefacts': {
                                'nodes_jsonl_gz': nodes_jsonl_gz_filename,
                                'equivalences_tsv': equivalences_tsv_filename,
                                'expanded_subjects_jsonl': expanded_subjects_jsonl_filename,
                                'sorted_expanded_subjects_jsonl': sorted_expanded_subjects_jsonl_filename,
                                'sorted_expanded_subjects_jsonl_gz': sorted_expanded_subjects_jsonl_gz_filename
                            }
                        }))

    print("Found " + str(len(datasource_files)) + " files to ingest")

    ###
    ### 1. Run ingest jobs
    ###
    datasource_files_listing = os.path.abspath( os.path.join(os.environ['GREBI_NFS_TMP'], os.environ['GREBI_CONFIG'], '01_ingest', 'datasource_files.jsonl') )
    os.makedirs(os.path.dirname(datasource_files_listing), exist_ok=True)

    with open(datasource_files_listing, 'w') as f2:
        f2.write('\n'.join(datasource_files))
    print("Files listing written to " + datasource_files_listing)
    print("Running ingest jobs for each file (" + str(len(datasource_files)) + " jobs)")

    if config['use_slurm'] == True:
        print("Running ingest on slurm (use_slurm = true)")
        slurm_cmd = ' '.join([
            'sbatch',
            '--wait',
            '-o ' + os.path.abspath(os.path.join(os.environ['GREBI_NFS_TMP'], os.environ['GREBI_CONFIG'], '01_ingest', 'ingest_%a.log')),
            '--array=0-' + str(len(datasource_files)-1) + '%' + str(config['slurm_max_workers']['ingest']),
            '--time=' + config['slurm_max_time']['ingest'],
            '--mem=' + config['slurm_max_memory']['ingest'],
            './01_ingest/grebi_ingest_worker.slurm.sh',
            datasource_files_listing
        ])
        res = os.system(slurm_cmd)
        os.system("tail -n +1 " + os.path.abspath(os.path.join(os.environ['GREBI_NFS_TMP'], os.environ['GREBI_CONFIG'], '01_ingest', '*.log')))
        if res != 0:
            print("Ingest failed")
            exit(1)
    else:
        print("Running ingest locally (use_slurm = false)")
        for n in range(len(datasource_files)):
            print("Running " + str(n) + " of " + str(len(datasource_files)))
            if os.system('SLURM_ARRAY_TASK_ID=' + str(n) + ' ./01_ingest/grebi_ingest_worker.slurm.sh ' + datasource_files_listing) != 0:
                print("Ingest failed")
                exit(1)
    os.sync()

    ###
    ### 2. Assign IDs to nodes (merging cliques)
    ###
    dir_to_search_for_equiv_files = os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], '01_ingest'))
    equiv_groups_txt = os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], '02_equivalences', 'groups.txt'))

    # 2.1. Build database of equivalence cliques
    if config['use_slurm'] == True:
        print("Building equivalence db on slurm (use_slurm = true)")
        slurm_cmd = ' '.join([
            'srun',
            '--time=' + config['slurm_max_time']['build_equiv_groups'],
            '--mem=' + config['slurm_max_memory']['build_equiv_groups'],
            './02_equivalences/grebi_build_equiv_groups.slurm.sh',
            dir_to_search_for_equiv_files,
            equiv_groups_txt
        ])
        if os.system(slurm_cmd) != 0:
            print("Failed to build equivalence groups")
            exit(1)
    else:
        print("Building equivalence db locally (use_slurm = false)")
        cmd = ' '.join([
            './02_equivalences/grebi_build_equiv_groups.slurm.sh',
            dir_to_search_for_equiv_files,
            equiv_groups_txt
        ])
        print(cmd)
        if os.system(cmd) != 0:
            print("Failed to build equivalence db")
            exit(1)
    os.sync()

    # 2.2. Assign IDs using the equivalences db
    if config['use_slurm'] == True:
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

    ###
    ### 3. Merge
    ###
    if config['use_slurm'] == True:
        print("Merging on slurm (use_slurm = true)")
        slurm_cmd = ' '.join([
            'srun',
            '--time=' + config['slurm_max_time']['merge'],
            '--mem=' + config['slurm_max_memory']['merge'],
            './03_merge/grebi_merge.slurm.sh',
            config_filename,
            datasource_files_listing
        ])
        if os.system(slurm_cmd) != 0:
            print("Failed to merge, command was: " + slurm_cmd)
            exit(1)
    else:
        print("Merging locally (use_slurm = false)")
        cmd = ' '.join([
            './03_merge/grebi_merge.slurm.sh',
            config_filename,
            datasource_files_listing
        ])
        print(cmd)
        if os.system(cmd) != 0:
            print("Failed to merge")
            exit(1)
    os.sync()

    ###
    ### 4. Index
    ###
    if config['use_slurm'] == True:
        print("Indexing on slurm (use_slurm = true)")
        slurm_cmd = ' '.join([
            'srun',
            '--time=' + config['slurm_max_time']['index'],
            '--mem=' + config['slurm_max_memory']['index'],
            './04_index/grebi_index.slurm.sh',
            config_filename
        ])
        if os.system(slurm_cmd) != 0:
            print("Failed to index")
            exit(1)
    else:
        print("Indexing locally (use_slurm = false)")
        cmd = ' '.join([
            './04_index/grebi_index.slurm.sh',
            config_filename
        ])
        print(cmd)
        if os.system(cmd) != 0:
            print("Failed to index")
            exit(1)
    os.sync()

    ###
    ### 5. Materialize edges
    ###
    input_merged_gz_filenames = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "03_merge", "merged.jsonl.0*")

    all_files = glob.glob(input_merged_gz_filenames)
    max_file_num = max(list(map(lambda f: int(f.split('.')[-2]), all_files)))
    print(get_time() + " --- Max file num: " + str(max_file_num))

    os.makedirs(os.path.join(os.environ['GREBI_NFS_TMP'], os.environ['GREBI_CONFIG'], '05_materialize_edges'), exist_ok=True)

    if config['use_slurm'] == True:
        print("Running materialize_edges on slurm (use_slurm = true)")
        slurm_cmd = ' '.join([
            'sbatch',
            '--wait',
            '-o ' + os.path.abspath(os.path.join(os.environ['GREBI_NFS_TMP'], os.environ['GREBI_CONFIG'], '05_materialize_edges', 'materialize_edges_%a.log')),
            '--array=0-' + str(max_file_num) + '%' + str(config['slurm_max_workers']['extract']),
            '--time=' + config['slurm_max_time']['extract'],
            '--mem=' + config['slurm_max_memory']['extract'],
            './05_materialize_edges/grebi_materialize_edges.slurm.sh',
            config_filename
        ])
        if os.system(slurm_cmd) != 0:
            print("materialize_edges failed")
            exit(1)
        os.system("tail -n +1 " + os.path.abspath(os.path.join(os.environ['GREBI_NFS_TMP'], os.environ['GREBI_CONFIG'], '05_materialize_edges', '*.log')))
    else:
        for n in range(max_file_num+1):
            print("Running " + str(n) + " of " + str(max_file_num))
            if os.system('SLURM_ARRAY_TASK_ID=' + str(n) + ' ./05_materialize_edges/grebi_materialize_edges.slurm.sh ' + config_filename) != 0:
                print("materialize_edges failed")
                exit(1)

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
   main()





