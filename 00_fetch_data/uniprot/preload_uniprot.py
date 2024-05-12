
import sys 
import glob
import os

def main():
    if len(sys.argv) < 2:
        print("Usage: preload_uniprot.py <uniprot_rdf_xz_path> <out_path>")
        exit(1)

    in_path = sys.argv[1]
    out_path = sys.argv[2]

    print("In RDF path: " + in_path)
    print("Out path: " + out_path)

    files = glob.glob(os.path.join(in_path, "*.xz"))

    print("Found " + str(len(files)) + " files")

    os.makedirs(out_path, exist_ok=True)

    listing_path = os.path.join(out_path, "files.txt")
    with open(listing_path, 'w') as f:
        f.write('\n'.join(files))

    slurm_cmd = ' '.join([
        'sbatch',
        '--wait',
        '-o ' + os.path.abspath(os.path.join(out_path, 'preload_uniprot_%a.log')),
        '--array=0-' + str(len(files)-1) + '%100',
        '--time=1:0:0',
        '--mem=150g',
        '-c 4',
        './00_fetch_data/uniprot/preload_uniprot.slurm.sh',
        out_path
    ])

    print(slurm_cmd)

    if os.system(slurm_cmd) != 0:
        print("preload_uniprot failed")
        exit(1)

    os.system("tail -n +1 " + out_path + '/*.log')

if __name__=="__main__":
   main()
