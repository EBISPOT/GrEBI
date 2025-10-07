#!/usr/bin/env python3
"""
Script to generate individual OLS datasource config files, one per ontology.
This enables parallel processing of OLS ontologies.
"""

import os
import yaml

# Define all ontologies and their configurations
# Each ontology will get its own config file
ONTOLOGY_CONFIGS = {
    # Main OLS ontologies
    'ols': {
        'ontologies': [
            'efo', 'mp', 'hp', 'go', 'ro', 'iao', 'uberon', 'pato', 'oba', 
            'chebi', 'bspo', 'obi', 'bfo', 'cob', 'cl', 'so', 'eco', 'pr', 
            'ncbitaxon', 'oio', 'biolink', 'mondo', 'doid', 'cheminf', 'dc', 
            'dcterms', 'ncit', 'edam', 'upheno', 'ecto'
        ],
        'input_file': 'dataload/00_fetch_data/ols/ontologies_linked.json.gz',
        'skip_obsolete': True,
        'defining_only': False
    },
    # HETT specific ontologies
    'ols_hett': {
        'ontologies': ['cheminf', 'chebi', 'edam', 'dc', 'dcterms', 'go', 'pato', 'ncit', 'ro'],
        'input_file': '/data/ontologies/ontologies.json',
        'skip_obsolete': True,
        'defining_only': False
    },
    # EFO only
    'ols_efo_only': {
        'ontologies': ['efo'],
        'input_file': '/data/ontologies/ontologies_linked.json.gz',
        'skip_obsolete': True,
        'defining_only': False
    },
    # IMPC x GWAS ontologies
    'ols_impc_x_gwas': {
        'ontologies': [
            'efo', 'mp', 'hp', 'ro', 'iao', 'uberon', 'pato', 'oba', 'bspo', 
            'obi', 'bfo', 'cob', 'oio', 'biolink', 'mondo', 'doid', 'dc', 
            'dcterms', 'ncit', 'edam', 'upheno'
        ],
        'input_file': 'dataload/00_fetch_data/ols/ontologies_linked.json.gz',
        'skip_obsolete': True,
        'defining_only': False
    }
}

def generate_config_for_ontology(config_group, ontology_id, input_file, skip_obsolete, defining_only):
    """Generate a YAML config for a single ontology."""
    config = {
        'name': f'OLS.{ontology_id}',
        'enabled': True,
        'ingests': [
            {
                'globs': [input_file],
                'command': (
                    f'grebi_ingest_ols\n'
                    f'  --datasource-name $GREBI_INGEST_DATASOURCE_NAME\n'
                    f'  --ontologies {ontology_id}'
                )
            }
        ]
    }
    
    # Add optional flags
    if skip_obsolete:
        config['ingests'][0]['command'] += '\n  --skip-obsolete'
    if defining_only:
        config['ingests'][0]['command'] += '\n  --defining-only'
    
    return config

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Track all generated files for each config group
    generated_files = {}
    
    for config_group, group_config in ONTOLOGY_CONFIGS.items():
        print(f"Generating configs for {config_group}...")
        generated_files[config_group] = []
        
        for ontology_id in group_config['ontologies']:
            # Generate config
            config = generate_config_for_ontology(
                config_group,
                ontology_id,
                group_config['input_file'],
                group_config['skip_obsolete'],
                group_config['defining_only']
            )
            
            # Write to file
            filename = f"ols_{ontology_id}.yaml"
            filepath = os.path.join(script_dir, filename)
            
            with open(filepath, 'w') as f:
                # Custom YAML formatting to match existing style
                f.write(f"name: {config['name']}\n")
                f.write(f"enabled: {str(config['enabled']).lower()}\n")
                f.write("ingests:\n")
                for ingest in config['ingests']:
                    f.write("  - globs: [")
                    glob_str = ', '.join([f'"{g}"' for g in ingest['globs']])
                    f.write(glob_str)
                    f.write("]\n")
                    f.write("    command: '\n")
                    for line in ingest['command'].split('\n'):
                        f.write(f"      {line}\n")
                    f.write("      '")
            
            generated_files[config_group].append(f"./configs/datasource_configs/{filename}")
            print(f"  Generated {filename}")
    
    print("\nGenerated files by config group:")
    for group, files in generated_files.items():
        print(f"\n{group}:")
        for f in sorted(files):
            print(f"  {f}")
    
    print("\n✓ Config generation complete!")
    print("\nNext steps:")
    print("1. Update subgraph config Python files to use the new individual configs")
    print("2. Run 'make' in dataload/configs/subgraph_configs/ to regenerate JSON configs")

if __name__ == '__main__':
    main()
