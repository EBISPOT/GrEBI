#!/usr/bin/env python3

import importlib.util
import io
import json
import math
import re
from pathlib import Path
import subprocess
import sys
import unittest
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ATLAS = load_module("atlas", "dataload/01_ingest/expression_atlas.py")
DISCOVERY = load_module("atlas_download", "dataload/00_download/expression_atlas.py")
ACC = "E-MTAB-513"
LIVER = "http://purl.obolibrary.org/obo/UBERON_0002107"
LUNG = "http://purl.obolibrary.org/obo/UBERON_0002048"
TAXON = "http://purl.obolibrary.org/obo/NCBITaxon_9606"


def sdrf(assay="a1", anatomy=LIVER, extra=(), taxon=TAXON):
    entries = [("organism", "Homo sapiens", taxon), ("organism part", "liver", anatomy)] + list(extra)
    return "".join(f"{ACC}\t\t{assay}\tcharacteristic\t{key}\t{value}\t{iri}\n"
                   for key, value, iri in entries)


def config(groups=(("g1", ["a1"]),), experiment_type="rnaseq_mrna_baseline"):
    root = ET.Element("configuration", experimentType=experiment_type)
    parent = ET.SubElement(ET.SubElement(root, "analytics"), "assay_groups")
    for name, assays in groups:
        node = ET.SubElement(parent, "assay_group", id=name, label=name)
        for assay in assays:
            ET.SubElement(node, "assay").text = assay
    return root


def run(table="GeneID\tGene Name\tg1\nENSG00000000003\tTSPAN6\t2\n", xml=None, metadata=None, threshold=0.5):
    output = io.StringIO()
    counts = ATLAS.ingest(io.StringIO(table), config() if xml is None else xml,
                          io.StringIO(sdrf() if metadata is None else metadata), ACC, threshold, output)
    return [json.loads(line) for line in output.getvalue().splitlines()], counts


def genes(nodes):
    return [node for node in nodes if node["grebi:type"] == "biolink:Gene"]


class ExpressionAtlasIngestTest(unittest.TestCase):
    def test_gene_anatomy_and_paired_evidence(self):
        nodes, counts = run(metadata=sdrf(extra=[("developmental stage", "adult", "")]))
        node = genes(nodes)[0]
        self.assertEqual(node["id"], "ensembl:ENSG00000000003")
        self.assertEqual(node["biolink:in_taxon"], "ncbitaxon:9606")
        edge = node["biolink:expressed_in"][0]
        self.assertEqual(edge["grebi:value"], "uberon:0002107")
        props = edge["grebi:properties"]
        self.assertEqual(props["expression_atlas:min_median_tpm"], [0.5])
        evidence = json.loads(node["expression_atlas:evidence"][0])
        self.assertEqual(evidence["median_tpm"], 2.0)
        self.assertEqual(evidence["context"], {"developmental stage": ["adult"]})
        self.assertEqual(evidence["assay_group"], "g1")
        self.assertEqual(evidence["study"], "https://www.ebi.ac.uk/gxa/experiments/E-MTAB-513")
        self.assertEqual(evidence["anatomy"], edge["grebi:value"])
        self.assertEqual(evidence["gene"], node["id"])
        self.assertEqual(evidence["taxon"], node["biolink:in_taxon"])
        self.assertEqual(counts["gene_anatomy_pairs"], 1)
        self.assertEqual(len(nodes), 3)  # provenance, anatomy, gene

    def test_threshold_uses_median_strictly_above_cutoff(self):
        table = "GeneID\tGene Name\tg1\n" + "\n".join([
            "ENSG00000000001\tA\t0,0.1,0.5,20,100",
            "ENSG00000000002\tB\t0,0.5,0.6,1,100",
            "ENSG00000000003\tC\t0",
            "ENSG00000000004\tD\tNA",
        ]) + "\n"
        nodes, _ = run(table)
        self.assertEqual([g["id"] for g in genes(nodes)], ["ensembl:ENSG00000000002"])
        self.assertFalse(genes(run(table, threshold=1)[0]))
        self.assertEqual(len(genes(run(table, threshold=0)[0])), 2)

    def test_duplicate_anatomy_merges_groups_without_losing_context(self):
        table = "GeneID\tGene Name\tg2\tg1\nENSG00000000003\tTSPAN6\t3\t2\n"
        xml = config((("g1", ["a1"]), ("g2", ["a2"])))
        metadata = sdrf() + sdrf("a2", extra=[("developmental stage", "embryonic", "")])
        nodes, counts = run(table, xml, metadata)
        edges = genes(nodes)[0]["biolink:expressed_in"]
        self.assertEqual(len(edges), 1)
        evidence = [json.loads(x) for x in genes(nodes)[0]["expression_atlas:evidence"]]
        self.assertEqual({x["assay_group"]: x["median_tpm"] for x in evidence}, {"g1": 2, "g2": 3})
        self.assertEqual(counts["supporting_group_results"], 2)

    def test_different_observations_have_identical_edge_values_for_merging(self):
        first = genes(run()[0])[0]
        second = genes(run("GeneID\tGene Name\tg1\nENSG00000000003\tTSPAN6\t5\n",
                           metadata=sdrf(extra=[("developmental stage", "embryonic", "")]))[0])[0]
        self.assertEqual(first["biolink:expressed_in"], second["biolink:expressed_in"])
        self.assertNotEqual(first["expression_atlas:evidence"], second["expression_atlas:evidence"])

    def test_excludes_perturbed_unknown_and_cell_line_samples(self):
        cases = [("disease", "cancer"), ("disease", "not available"),
                 ("cell line", "HepG2"), ("progenitor cell line", "iPSC-1"),
                 ("genotype", "knockout"), ("compound", "lipopolysaccharide"),
                 ("treatment", "unknown"), ("infection", "infected")]
        for key, value in cases:
            with self.subTest(key=key, value=value):
                nodes, counts = run(metadata=sdrf(extra=[(key, value, "")]))
                self.assertFalse(genes(nodes))
                self.assertEqual(len(nodes), 1)  # provenance keeps the ingest nonempty
                self.assertEqual(counts["included_groups"], 0)

    def test_controls_are_accepted(self):
        for key, value in [("disease", "normal"), ("genotype", "wild type genotype"),
                           ("compound", "PBS control"), ("treatment", "untreated")]:
            with self.subTest(key=key):
                self.assertEqual(len(genes(run(metadata=sdrf(extra=[(key, value, "")]))[0])), 1)

    def test_one_excluded_replicate_excludes_whole_group(self):
        xml = config((("g1", ["a1", "a2"]),))
        metadata = sdrf() + sdrf("a2", extra=[("disease", "cancer", "")])
        self.assertFalse(genes(run(xml=xml, metadata=metadata)[0]))

    def test_mixed_unmapped_and_cell_type_anatomy_are_not_guessed(self):
        for anatomy in ("", "CL:0000236", "http://purl.obolibrary.org/obo/CL_0000236"):
            with self.subTest(anatomy=anatomy):
                self.assertFalse(genes(run(metadata=sdrf(anatomy=anatomy))[0]))
        xml = config((("g1", ["a1", "a2"]),))
        self.assertFalse(genes(run(xml=xml, metadata=sdrf() + sdrf("a2", LUNG))[0]))
        self.assertFalse(genes(run(metadata=sdrf(taxon=""))[0]))

    def test_plant_anatomy_and_native_gene_aliases(self):
        table = "GeneID\tGene Name\tg1\nAT1G01010\tNAC001\t4\n"
        nodes, _ = run(table, metadata=sdrf(anatomy="http://purl.obolibrary.org/obo/PO_0009005",
                                           taxon="http://purl.obolibrary.org/obo/NCBITaxon_3702"))
        node = genes(nodes)[0]
        self.assertEqual(node["biolink:expressed_in"][0]["grebi:value"], "po:0009005")
        self.assertEqual(node["dcterms:identifier"], ["tair.locus:AT1G01010"])
        self.assertEqual(ATLAS.gene_node("WBGene00000001", "", "ncbitaxon:6239")["dcterms:identifier"], ["wormbase:WBGene00000001"])

    def test_rejects_invalid_thresholds_and_expression_values(self):
        for value in (-1, math.inf, math.nan):
            with self.subTest(value=value), self.assertRaises(ValueError):
                run(threshold=value)
        for value in ("-1", "inf", "2,1,0,3,4", "1,2", "garbage", "1,2,NaN,4,5"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ATLAS.median_tpm(value)

    def test_rejects_schema_mismatch_duplicate_and_truncated_rows(self):
        for table in ("", "GeneID\tGene Name\tg1\n", "TranscriptID\tGene Name\tg1\nx\tx\t2\n",
                      "GeneID\tGene Name\tg2\nx\tx\t2\n", "GeneID\tGene Name\tg1\nx\tx\n",
                      "GeneID\tGene Name\tg1\nx\tx\t2\nx\tx\t3\n"):
            with self.subTest(table=table), self.assertRaises(ValueError):
                run(table)
        with self.assertRaisesRegex(ValueError, "missing from"):
            run(xml=config((("g1", ["missing"]),)))
        with self.assertRaisesRegex(ValueError, "baseline"):
            run(xml=config(experiment_type="rnaseq_mrna_differential"))
        with self.assertRaisesRegex(ValueError, "Malformed"):
            run(metadata="wrong\trow\n")

    def test_cli_requires_threshold_and_fails_missing_companions(self):
        script = ROOT / "dataload/01_ingest/expression_atlas.py"
        result = subprocess.run([sys.executable, str(script), "E-MTAB-513-tpms.tsv"], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--min-median-tpm", result.stderr)
        result = subprocess.run([sys.executable, str(script), "--min-median-tpm", "0.5", "missing/E-MTAB-513-tpms.tsv"], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("failed", result.stderr)

    def test_download_manifest_selects_only_baseline_gene_files(self):
        catalogue = {"experiments": [
            {"experimentAccession": "E-MTAB-513", "rawExperimentType": "RNASEQ_MRNA_BASELINE"},
            {"experimentAccession": "E-MTAB-999", "rawExperimentType": "RNASEQ_MRNA_DIFFERENTIAL"},
            {"experimentAccession": "E-PROT-1", "rawExperimentType": "PROTEOMICS_BASELINE"},
        ]}
        roots = ["/nfs/atlas/experiments/", "https://ftp.ebi.ac.uk/atlas/experiments"]
        entries = DISCOVERY.download_entries(catalogue, roots)
        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0], {
            "dest": "expression_atlas/experiments/E-MTAB-513/E-MTAB-513-tpms.tsv",
            "sources": ["/nfs/atlas/experiments/E-MTAB-513/E-MTAB-513-tpms.tsv",
                        "https://ftp.ebi.ac.uk/atlas/experiments/E-MTAB-513/E-MTAB-513-tpms.tsv"],
        })
        rendered = json.dumps(entries)
        for absent in ("E-MTAB-999", "E-PROT-1", "fpkms", "transcripts", "coexpressions", "aggregated_counts"):
            self.assertNotIn(absent, rendered)

    def test_config_discovers_files_and_keeps_threshold_in_yaml(self):
        text = (ROOT / "configs/datasource_configs/expression_atlas.yaml").read_text()
        self.assertIn("download_manifests:", text)
        self.assertIn("download_manifest: expression_atlas/downloads.json", text)
        self.assertIn("--min-median-tpm 0.5", text)
        self.assertIn("/nfs/ftp/public/databases/microarray/data/atlas/experiments.json", text)
        self.assertIn("https://ftp.ebi.ac.uk/pub/databases/microarray/data/atlas/experiments.json", text)
        self.assertFalse(re.search(r"E-[A-Z]+-\d+", text))
        self.assertLess(len(text.splitlines()), 30)

    def test_discovery_tracks_added_removed_and_reclassified_studies(self):
        first = {"experimentAccession": "E-TEST-1", "rawExperimentType": "RNASEQ_MRNA_BASELINE"}
        second = {"experimentAccession": "E-TEST-2", "rawExperimentType": "RNASEQ_MRNA_BASELINE"}
        catalogue = {"experiments": [first]}
        self.assertEqual(len(DISCOVERY.download_entries(catalogue, ["/atlas"])), 3)
        catalogue["experiments"].append(second)
        self.assertEqual(len(DISCOVERY.download_entries(catalogue, ["/atlas"])), 6)
        first["rawExperimentType"] = "RNASEQ_MRNA_DIFFERENTIAL"
        entries = DISCOVERY.download_entries(catalogue, ["/atlas"])
        self.assertEqual(len(entries), 3)
        self.assertTrue(all("E-TEST-2" in entry["dest"] for entry in entries))
        catalogue["experiments"].remove(first)
        self.assertEqual(entries, DISCOVERY.download_entries(catalogue, ["/atlas"]))

    def test_discovery_rejects_malformed_empty_and_duplicate_catalogues(self):
        experiment = {"experimentAccession": "E-TEST-1", "rawExperimentType": "RNASEQ_MRNA_BASELINE"}
        for catalogue in (None, [], {}, {"experiments": {}}, {"experiments": []},
                          {"experiments": [None]}, {"experiments": [{}]},
                          {"experiments": [experiment, experiment]}):
            with self.subTest(catalogue=catalogue), self.assertRaises(ValueError):
                DISCOVERY.download_entries(catalogue, ["/atlas"])
        for accession in (None, 123, "../E-TEST-1", "E-TEST-1/../../escape", 'E-TEST-1";touch bad'):
            with self.subTest(accession=accession), self.assertRaises(ValueError):
                DISCOVERY.download_entries({"experiments": [{**experiment, "experimentAccession": accession}]}, ["/atlas"])
        for roots in ([], [""], [None]):
            with self.subTest(roots=roots), self.assertRaises(ValueError):
                DISCOVERY.download_entries({"experiments": [experiment]}, roots)

    def test_discovery_cli_produces_parseable_manifest_and_stderr_summary(self):
        result = subprocess.run([
            sys.executable, str(ROOT / "dataload/00_download/expression_atlas.py"),
            "--source-root", "tests/data/test_expression_atlas",
            str(ROOT / "tests/data/test_expression_atlas/experiments.json"),
        ], capture_output=True, text=True, check=True)
        entries = json.loads(result.stdout)
        self.assertEqual(len(entries), 9)
        self.assertIn("3 baseline studies (9 files)", result.stderr)
        for entry in entries:
            self.assertTrue((ROOT / entry["sources"][0]).is_file())

    def test_real_human_and_plant_fixtures(self):
        base = ROOT / "tests/data/test_expression_atlas"
        for accession, pairs in [("E-MTAB-513", 8), ("E-MTAB-4045", 4), ("E-TEST-1", 1)]:
            with self.subTest(accession=accession):
                directory = base / accession
                with (directory / (accession + "-tpms.tsv")).open() as table, (directory / (accession + ".condensed-sdrf.tsv")).open() as sdrf_file:
                    output = io.StringIO()
                    result = ATLAS.ingest(table, ET.parse(directory / (accession + "-configuration.xml")).getroot(),
                                          sdrf_file, accession, 0.5, output)
                self.assertEqual(result["gene_anatomy_pairs"], pairs)


if __name__ == "__main__":
    unittest.main()
