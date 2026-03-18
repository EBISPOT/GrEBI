
import sys 
import glob
import os
import shlex

def main():
    if len(sys.argv) < 2:
        print("Usage: preload_uniprot.slurm.py <out_path>")
        exit(1)

    out_path = sys.argv[1]

    listing_path = os.path.join(out_path, "files.txt")
    with open(listing_path, 'r') as f:
        files = f.read().splitlines()
        our_file = files[int(os.environ['SLURM_ARRAY_TASK_ID'])]

    print("Our file: " + our_file)

    # Ingest converts from RDF to JSONL, flattening bnodes and handling reification
    cmd = ' '.join([
        'xzcat', shlex.quote(our_file),
        '|',
        './target/release/grebi_ingest_rdf',
        '--datasource-name UniProt',
        '--rdf-type rdf_triples_xml',
        '--nest-objects-of-predicate http://purl.uniprot.org/core/attribution',
        '--nest-objects-of-predicate http://purl.uniprot.org/core/encodedBy',
        '--nest-objects-of-predicate http://purl.uniprot.org/core/recommendedName',
        '--nest-objects-of-predicate http://purl.uniprot.org/core/alternativeName',
        '--nest-objects-of-predicate http://purl.uniprot.org/core/locatedIn',
        '--exclude-objects-of-predicate http://purl.uniprot.org/core/annotation',
        '--exclude-objects-of-predicate http://purl.uniprot.org/core/sequence',
        '--exclude-objects-of-predicate http://purl.uniprot.org/core/range',
        '--exclude-objects-of-predicate http://biohackathon.org/resource/faldo#begin',
        '--exclude-objects-of-predicate http://biohackathon.org/resource/faldo#end',
        '| ./target/release/grebi_normalise_prefixes',
        '| pigz --fast',
        '>',
        shlex.quote(os.path.join(out_path, os.path.basename(our_file) + '.jsonl.gz'))
    ])
    if os.system('bash -c "' + cmd + '"') != 0:
        print("UniProt preload ingest failed")
        exit(1)

if __name__=="__main__":
   main()