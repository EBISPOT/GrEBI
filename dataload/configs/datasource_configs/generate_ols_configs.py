#!/usr/bin/env python3
"""
Script to generate individual OLS datasource config files, one per ontology.
This enables parallel processing of OLS ontologies.
"""

import os

# All unique ontologies across all OLS configs
# Using the most common input file path
ALL_ONTOLOGIES = [
    'efo', 'mp', 'hp', 'go', 'ro', 'iao', 'uberon', 'pato', 'oba', 
    'chebi', 'bspo', 'obi', 'bfo', 'cob', 'cl', 'so', 'eco', 'pr', 
    'ncbitaxon', 'oio', 'biolink', 'mondo', 'doid', 'cheminf', 'dc', 
    'dcterms', 'ncit', 'edam', 'upheno', 'ecto'
]

DEFAULT_INPUT_FILE = 'dataload/00_fetch_data/ols/ontologies_linked.json.gz'

def generate_config_for_ontology(ontology_id):
    """Generate a YAML config for a single ontology."""
    config = {
        'name': f'OLS.{ontology_id}',
        'enabled': True,
        'ingests': [
            {
                'globs': [DEFAULT_INPUT_FILE],
                'command': (
                    f'grebi_ingest_ols\n'
                    f'  --datasource-name $GREBI_INGEST_DATASOURCE_NAME\n'
                    f'  --ontologies {ontology_id}\n'
                    f'  --skip-obsolete'
                )
            }
        ]
    }
    
    return config

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    generated_files = []
    
    print("Generating individual OLS configs...")
    
    for ontology_id in ALL_ONTOLOGIES:
        # Generate config
        config = generate_config_for_ontology(ontology_id)
        
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
        
        generated_files.append(filename)
        print(f"  Generated {filename}")
    
    print(f"\n✓ Generated {len(generated_files)} OLS config files")
    print("\nNext steps:")
    print("1. Update subgraph config Python files to use the new individual configs")
    print("2. Run 'make' in dataload/configs/subgraph_configs/ to regenerate JSON configs")

if __name__ == '__main__':
    main()
