#!/usr/bin/env python3

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "dataload" / "01_ingest" / "biostudies.py"
SPEC = importlib.util.spec_from_file_location("biostudies_ingest", MODULE_PATH)
BIOSTUDIES = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(BIOSTUDIES)


class BioStudiesIngestTest(unittest.TestCase):
    def test_extracts_graph_metadata_but_not_submission_files(self):
        fixture = (
            ROOT
            / "tests/data/test_biostudies/fire/S-BSST/001/S-BSST1/S-BSST1.json"
        )
        node = BIOSTUDIES.parse_submission(json.loads(fixture.read_text()))

        self.assertEqual(node["id"], "biostudies:S-BSST1")
        self.assertEqual(node["grebi:type"], "biostudies:Study")
        self.assertEqual(node["dcterms:issued"], "2016-10-13")
        self.assertEqual(node["biostudies:organism"], ["Homo sapiens"])
        self.assertEqual(node["biostudies:ontologyTerm"], ["NCBITaxon_9606"])
        self.assertEqual(node["dcterms:creator"], ["Dr Michael Cox"])
        self.assertEqual(node["biostudies:organisation"], ["Imperial College London"])
        self.assertEqual(
            node["dcterms:references"],
            ["insdc:PRJEB14304"],
        )

        serialised = json.dumps(node)
        self.assertNotIn("LAMB_denoised_reads.fna", serialised)
        self.assertNotIn("397823226", serialised)
        self.assertNotIn("mjcox@ic.ac.uk", serialised)
        self.assertNotIn("ftp://", serialised)

    def test_extracts_publication_and_collection_identifiers(self):
        node = BIOSTUDIES.parse_submission(
            {
                "accno": "E-MTAB-1",
                "attributes": [
                    {"name": "Title", "value": "Example study"},
                    {"name": "AttachTo", "value": "ArrayExpress"},
                ],
                "section": {
                    "type": "Study",
                    "attributes": [
                        {
                            "name": "Study type",
                            "value": "transcription profiling by array",
                            "valqual": [
                                {"name": "Ontology", "value": "EFO"},
                                {"name": "TermId", "value": "EFO_0002768"},
                            ],
                        }
                    ],
                    "subsections": [
                        {
                            "accno": "20435134",
                            "type": "Publication",
                            "attributes": [
                                {"name": "DOI", "value": "10.1016/j.ygeno.2010.04.004"}
                            ],
                        }
                    ],
                },
            }
        )

        self.assertEqual(node["biostudies:collection"], ["biostudies:ArrayExpress"])
        self.assertEqual(node["dcterms:identifier"], ["arrayexpress:E-MTAB-1"])
        self.assertEqual(node["biostudies:ontologyTerm"], ["EFO_0002768"])
        self.assertEqual(
            node["dcterms:references"],
            ["pubmed:20435134", "doi:10.1016/j.ygeno.2010.04.004"],
        )

    def test_directory_walk_prunes_files_and_europe_pmc(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            included = root / "S-BSST" / "001" / "S-BSST1"
            linked_files = included / "Files"
            excluded = root / "S-EPMC" / "001" / "S-EPMC1"
            linked_files.mkdir(parents=True)
            excluded.mkdir(parents=True)
            (included / "S-BSST1.json").write_text("{}")
            (linked_files / "payload.json").write_text("{}")
            (excluded / "S-EPMC1.json").write_text("{}")

            found = list(BIOSTUDIES._page_tab_files(root))

        self.assertEqual(found, [included / "S-BSST1.json"])

    def test_europe_pmc_submission_is_excluded(self):
        self.assertIsNone(
            BIOSTUDIES.parse_submission(
                {"accno": "S-EPMC123", "attributes": [], "section": {"type": "Study"}}
            )
        )


if __name__ == "__main__":
    unittest.main()
