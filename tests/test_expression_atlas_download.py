#!/usr/bin/env python3

import importlib.util
import json
import re
from pathlib import Path
import subprocess
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


DISCOVERY = load_module("atlas_download", "dataload/00_download/expression_atlas.py")


class ExpressionAtlasDownloadTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
