import polars as pl
import hashlib
import json
import os

from ncbitaxon_local import load_ncbitaxon, get_labels_and_synonyms, get_ancestors, get_by_name

load_ncbitaxon()

os.makedirs('out', exist_ok=True)
nodes = open('out/mgnify_nodes.jsonl', 'w')
edges = open('out/mgnify_edges.jsonl', 'w')

wrote_nodes = set()

def main():

    studies = pl.read_csv('data/studies.tsv', separator='\t')
    samples = pl.read_csv('data/samples.tsv', separator='\t')

    n = 0

    for study in studies.iter_rows(named=True):
        print("Processing study number ", n, ":", study["study_accession"])
        n += 1

        acc = "ena.embl:" + study["study_accession"]
        pmids = study["pubmed_ids"].split(',') if study["pubmed_ids"] is not None else []
        summaries = study["study_summary_files"].split(',') if study["study_summary_files"] is not None else []

        summaries = list(map(load_summary, summaries)) if summaries else []

        nodes.write(json.dumps({
            "id": acc,
            "type": "mgnify:Study",
            "pubmed_ids": ["pmid:" + pmid for pmid in pmids]
        }) + '\n')


def load_summary(summary_file):
    filename = "data/" + hashlib.md5(summary_file.strip().encode()).hexdigest() + '.tsv'

    print("Loading ", summary_file + " hashed " + filename)

    if not os.path.isfile(filename):
        print("Missing file:", summary_file + " hashed " + filename)
        return summary_file

    df = pl.read_csv(filename, separator='\t')

    cols = df.columns

    if cols[0] == '#SampleID':
        for sample in df.iter_rows(named=True):
            taxon = write_taxon(sample[cols[0]])
            for col in cols[1:]:
                value = sample[col]
                if value is None:
                    continue
                value = str(value).strip()
                if not value or value == "0":
                    continue
                edges.write(json.dumps({
                    "id": ena_accession(col),
                    "mgnify:taxon": {
                        "grebi:value": taxon,
                        "grebi:properties": {
                            "mgnify:n": value
                        }
                    }
                }) + '\n')

    elif cols[0] == 'superkingdom' and cols[1] == 'kingdom' and cols[2] == 'phylum':
        for sample in df.iter_rows(named=True):
            taxon = write_taxon(sample["superkingdom"] + '; ' + sample["kingdom"] + '; ' + sample["phylum"])
            for col in cols[3:]:
                value = sample[col]
                if value is None:
                    continue
                value = str(value).strip()
                if not value or value == "0":
                    continue
                edges.write(json.dumps({
                    "id": ena_accession(col),
                    "mgnify:taxon": {
                        "grebi:value": taxon,
                        "grebi:properties": {
                            "mgnify:n": value
                        }
                    }
                }) + '\n')

    elif cols[0] == 'IPR' and cols[1] == 'description':
        for domain in df.iter_rows(named=True):
            for col in cols[2:]:
                value = domain[col]
                if value is None:
                    continue
                value = str(value).strip()
                if not value or value == "0":
                    continue
                if domain["IPR"] not in wrote_nodes:
                    wrote_nodes.add(domain["IPR"])
                    nodes.write(json.dumps({
                        "id": 'interpro:' + domain["IPR"],
                        "grebi:type": 'owl:Class',
                        "rdfs:label": domain["description"]
                    }) + '\n')
                edges.write(json.dumps({
                    "id": ena_accession(col),
                    "mgnify:feature": {
                        "grebi:value": 'interpro:' + domain["IPR"],
                        "grebi:properties": {
                            "mgnify:n": value
                        }
                    }
                }) + '\n')

    elif cols[0] == 'GO' and cols[1] == 'description' and cols[2] == 'category':
        for go in df.iter_rows(named=True):
            for col in cols[3:]:
                value = go[col]
                if value is None:
                    continue
                value = str(value).strip()
                if not value or value == "0":
                    continue
                if go["GO"] not in wrote_nodes:
                    wrote_nodes.add(go["GO"])
                    nodes.write(json.dumps({
                        "id": 'go:' + go["GO"].replace('GO:', ''),
                        "grebi:type": 'owl:Class',
                        "rdfs:label": go["description"]
                    }) + '\n')
                edges.write(json.dumps({
                    "id": ena_accession(col),
                    "mgnify:feature": {
                        "grebi:value": go["GO"].replace('GO:', 'go:'),
                        "grebi:properties": {
                            "mgnify:n": value
                        }
                    }
                }) + '\n')

    else:
        print("Unknown summary file format:", summary_file + " hashed " + filename)
        return summary_file
        
    return filename


def ena_accession(id):
    return 'ena.embl:' + id.split('_')[0]


def write_taxon(taxon):
    if taxon == 'Unclassified':
        return
    ncbi_taxon = map_taxon([i.strip() for i in taxon.split(';')])
    if ncbi_taxon is not None:
        taxa_out = {'id': 'ncbitaxon:' + ncbi_taxon, 'grebi:type': 'biolink:OrganismTaxon'}
        if ncbi_taxon not in wrote_nodes:
            wrote_nodes.add(ncbi_taxon)
            nodes.write(json.dumps(taxa_out) + '\n')
        return 'ncbitaxon:' + ncbi_taxon
    else:
        taxa_out = {'id': taxon, 'grebi:type': 'biolink:OrganismTaxon'}
        most_specific_superclass = None
        superclasses = [i.strip() for i in taxon.split(';')]
        while most_specific_superclass is None:
            superclasses = superclasses[:-1]
            if not superclasses:
                break
            most_specific_superclass = map_taxon(superclasses)
            if most_specific_superclass is not None:
                taxa_out['skos:broadMatch'] = 'ncbitaxon:' + most_specific_superclass
        if taxon not in wrote_nodes:
            wrote_nodes.add(taxon)
            nodes.write(json.dumps(taxa_out) + '\n')
        return taxon


def map_taxon(taxon):
    labels = [label.split('__')[-1].replace('_', ' ') for label in taxon]
    hits = get_by_name(labels[-1])
    if not hits:
        return None
    for hit in hits:
        ancestors = get_ancestors(hit)
        all_ancestor_labels = get_labels_and_synonyms(hit)
        for ancestor in ancestors:
            all_ancestor_labels.extend(get_labels_and_synonyms(ancestor))
        all_ancestor_labels = [label.lower() for label in all_ancestor_labels]
        if all(
            label.lower() in all_ancestor_labels
            or label in ("", "Unassigned")
            for label in labels
        ):
            return hit
    return None


if __name__ == "__main__":
    main()
