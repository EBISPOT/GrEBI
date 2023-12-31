
import json
import os
import glob
import sys
import subprocess
import shlex
import re

def main():

    if len(sys.argv) < 2:
        print("Usage: grebi_pipeline.py <grebi_config.json>")
        exit(1)

    config_filename = os.path.abspath(sys.argv[1])

    with open(config_filename, 'r') as f:
        config = json.load(f)

    datasource_files = []

    for datasource in config['datasources']:
        if datasource['enabled'] != True:
            print("Skipping disabled datasource: " + datasource['name'])
        else:
            for g in datasource['ingest_files']:
                files = glob.glob(g)
                for file in files:
                    filename = os.path.abspath(file)
                    basename = os.path.splitext(os.path.basename(filename))[0]
                    nodes_jsonl_filename = os.path.abspath( os.path.join(config['worker_output_dir'], '01_ingest', datasource['name'], basename + '.jsonl' ))
                    sorted_nodes_jsonl_filename = os.path.abspath( os.path.join(config['worker_output_dir'], '01_ingest', datasource['name'], basename + '.sorted.jsonl' ))
                    sorted_nodes_jsonl_gz_filename = os.path.abspath( os.path.join(config['worker_output_dir'], '01_ingest', datasource['name'], basename + '.sorted.jsonl.gz' ))
                    equivalences_tsv_filename = os.path.abspath( os.path.join(config['worker_output_dir'], '01_ingest', datasource['name'], basename  + '.equivalences.tsv'  ))
                    expanded_subjects_jsonl_filename = os.path.abspath( os.path.join(config['worker_output_dir'], '02_equivalences', datasource['name'], basename + '.expanded.jsonl' ))
                    sorted_expanded_subjects_jsonl_filename = os.path.abspath( os.path.join(config['worker_output_dir'], '02_equivalences', datasource['name'], basename + '.sorted_expanded.jsonl' ))
                    sorted_expanded_subjects_jsonl_gz_filename = os.path.abspath( os.path.join(config['worker_output_dir'], '02_equivalences', datasource['name'], basename + '.sorted_expanded.jsonl.gz' ))
                    datasource_files.append(json.dumps({
                        'config': config_filename,
                        'datasource': datasource,
                        'filename': filename,
                        'artefacts': {
                            'nodes_jsonl': nodes_jsonl_filename,
                            'sorted_nodes_jsonl': sorted_nodes_jsonl_filename,
                            'sorted_nodes_jsonl_gz': sorted_nodes_jsonl_gz_filename,
                            'equivalences_tsv': equivalences_tsv_filename,
                            'expanded_subjects_jsonl': expanded_subjects_jsonl_filename,
                            'sorted_expanded_subjects_jsonl': sorted_expanded_subjects_jsonl_filename,
                            'sorted_expanded_subjects_jsonl_gz': sorted_expanded_subjects_jsonl_gz_filename
                        }
                    }))


    ###
    ### 1. Run ingest jobs
    ###
    datasource_files_listing = os.path.abspath( os.path.join(config['persistent_output_dir'], '01_ingest', 'datasource_files.jsonl') )
    os.makedirs(os.path.dirname(datasource_files_listing), exist_ok=True)

    with open(datasource_files_listing, 'w') as f2:
        f2.write('\n'.join(datasource_files))
    print("Files listing written to " + datasource_files_listing)
    print("Running ingest jobs for each file (" + str(len(datasource_files)) + " jobs)")

    if config['use_slurm'] == True:
        print("Running ingest on slurm (use_slurm = true)")
        slurm_cmd = ' '.join([
            'sbatch',
            '--array=0-' + str(len(datasource_files)-1) + '%' + str(config['slurm_max_workers']),
            '--time=' + config['slurm_max_time']['ingest'],
            '--mem=' + config['slurm_max_memory']['ingest'],
            './01_ingest/grebi_ingest_worker.slurm.sh',
            datasource_files_listing
        ])
        print(slurm_cmd)
    else:
        print("Running ingest locally (use_slurm = false)")
        for n in range(len(datasource_files)):
            print("Running " + str(n) + " of " + str(len(datasource_files)))
            # os.system('SLURM_ARRAY_TASK_ID=' + str(n+1) + ' ./01_ingest/grebi_ingest_worker.slurm.sh ' + datasource_files_listing)
    os.sync()


    ###
    ### 2. Expand subjects with equivalences; this step also assigns IDs to entities
    ###
    dir_to_search_for_equiv_files = os.path.abspath(os.path.join(config['worker_output_dir'], '01_ingest'))
    equiv_rocksdb_path = os.path.abspath(os.path.join(config['worker_output_dir'], '02_equivalences', 'equivalences_db'))

    # 2.1. Build database of equivalences
    if config['use_slurm'] == True:
        print("Building equivalence db on slurm (use_slurm = true)")
        slurm_cmd = ' '.join([
            'sbatch',
            '--time=' + config['slurm_max_time']['build_equiv_db'],
            '--mem=' + config['slurm_max_memory']['build_equiv_db'],
            './02_equivalences/grebi_build_equiv_db.slurm.sh',
            equiv_rocksdb_path,
            dir_to_search_for_equiv_files
        ])
        print(slurm_cmd)
    else:
        print("Building equivalence db locally (use_slurm = false)")
        cmd = ' '.join([
            './02_equivalences/grebi_build_equiv_db.slurm.sh',
            equiv_rocksdb_path,
            dir_to_search_for_equiv_files
        ])
        # os.system(cmd)
    os.sync()

    # 2.2. Expand subjects using the equivalences db
    if config['use_slurm'] == True:
        print("Expanding subjects on slurm (use_slurm = true)")
        slurm_cmd = ' '.join([
            'sbatch',
            '--array=0-' + str(len(datasource_files)-1) + '%' + str(config['slurm_max_workers']),
            '--time=' + config['slurm_max_time']['expand_subjects'],
            '--mem=' + config['slurm_max_memory']['expand_subjects'],
            './02_equivalences/grebi_expand_subjects_worker.slurm.sh',
            datasource_files_listing,
            equiv_rocksdb_path
        ])
        print(slurm_cmd)
    else:
        print("Expanding subjects locally (use_slurm = false)")
        for n in range(len(datasource_files)):
            print("Running " + str(n) + " of " + str(len(datasource_files)))
            cmd = ' '.join([
                'SLURM_ARRAY_TASK_ID=' + str(n+1),
                './02_equivalences/grebi_expand_subjects_worker.slurm.sh',
                datasource_files_listing,
                equiv_rocksdb_path
            ])
            print(cmd)
            # os.system(cmd)
    os.sync()


    ###
    ### 3. Merge
    ###
    if config['use_slurm'] == True:
        print("Merging on slurm (use_slurm = true)")
        slurm_cmd = ' '.join([
            'sbatch',
            '--time=' + config['slurm_max_time']['ingest'],
            '--mem=' + config['slurm_max_memory']['ingest'],
            './03_merge/grebi_merge.slurm.sh',
            config_filename,
            datasource_files_listing
        ])
        print(slurm_cmd)
    else:
        print("Merging locally (use_slurm = false)")
        cmd = ' '.join([
            './03_merge/grebi_merge.slurm.sh',
            config_filename,
            datasource_files_listing
        ])
        print(cmd)
        os.system(cmd)
    os.sync()


if __name__=="__main__":
   main()





