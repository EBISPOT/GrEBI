#!/usr/bin/env python3

import importlib.util
import io
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pride_ingest", ROOT / "dataload/01_ingest/pride.py"
)
PRIDE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(PRIDE)


class PRIDEIngestTest(unittest.TestCase):
    def test_project_aliases_are_separate_from_publication_references(self):
        fixture = json.loads((ROOT / "tests/data/test_pride/projects.json").read_text())
        node = PRIDE.parse_project(fixture[0])
        self.assertEqual(node["id"], "pride.project:PXD001357")
        self.assertEqual(node["grebi:type"], "pride:Project")
        self.assertEqual(
            node["dcterms:identifier"], ["px:PXD001357", "doi:10.6019/PXD001357"]
        )
        self.assertEqual(node["dcterms:submitted"], "2014-10-15")
        self.assertEqual(node["dcterms:issued"], "2015-02-25")
        self.assertEqual(node["pride:organism"], [{
            "grebi:value": "ncbitaxon:9606",
            "grebi:properties": {"rdfs:label": ["Homo sapiens (human)"]},
        }])
        self.assertEqual(node["pride:instrument"][0]["grebi:value"], "MS:1001910")
        self.assertEqual(node["dcterms:references"], [
            "pubmed:25429530", "doi:10.1038/srep07104",
            "px:PXD001359", "pride.project:PXD001359",
        ])

    def test_metadata_whitelist_excludes_files_counters_and_email(self):
        node = PRIDE.parse_project({
            "accession": "PXD001357", "projectDescription": "A study",
            "sampleProcessingProtocol": "Sample protocol",
            "dataProcessingProtocol": "Data protocol",
            "license": "CC0", "submissionType": "COMPLETE",
            "keywords": ["proteomics", "proteomics", ""], "projectTags": ["Human"],
            "softwares": [{"accession": "MS:1001207", "name": "Mascot"}],
            "experimentTypes": [{"accession": "PRIDE:0000429", "name": "Shotgun proteomics"}],
            "quantificationMethods": [{"name": "Label-free"}],
            "identifiedPTMStrings": [{"accession": "UNIMOD:35", "name": "Oxidation"}],
            "submitters": [{
                "name": "Test Person", "orcid": "https://orcid.org/0000-0002-1825-0097",
                "affiliation": "Test institute", "email": "test@example.org",
            }],
            "labPIs": [{"firstName": "Test", "lastName": "PI"}],
            "files": [{"fileName": "experiment.raw", "downloadLink": "ftp://example.org/file"}],
            "totalFileDownloads": 987654321, "botCount": 987654322,
            "additionalAttributes": [{"name": "email", "value": "contact@example.org"}],
        })
        self.assertEqual(node["grebi:description"], "A study")
        self.assertEqual(node["pride:sampleProcessingProtocol"], "Sample protocol")
        self.assertEqual(node["pride:dataProcessingProtocol"], "Data protocol")
        self.assertEqual(node["pride:keyword"], ["proteomics"])
        self.assertEqual(node["pride:tag"], ["Human"])
        self.assertEqual(node["pride:software"][0]["grebi:value"], "MS:1001207")
        self.assertEqual(node["pride:modification"][0]["grebi:value"], "UNIMOD:35")
        self.assertEqual(node["pride:quantificationMethod"], ["Label-free"])
        self.assertEqual(node["dcterms:creator"], [{
            "grebi:value": "orcid:0000-0002-1825-0097",
            "grebi:properties": {"rdfs:label": ["Test Person"]},
        }, "Test PI"])
        self.assertEqual(node["pride:organisation"], ["Test institute"])
        for excluded in ("experiment.raw", "ftp://", "987654321", "987654322", "@example.org"):
            self.assertNotIn(excluded, json.dumps(node))

    def test_sample_annotations_and_cross_references(self):
        node = PRIDE.parse_project({
            "accession": "PXD001357",
            "organisms": [{"accession": "NEWT:9606", "name": "Human"}],
            "sampleAttributes": [
                {"key": {"name": "organism"}, "value": [{"accession": "NEWT:9606", "name": "Human"}]},
                {"key": {"name": "organism part"}, "value": [{"accession": "BTO:0000338", "name": "Dental plaque"}]},
                {"key": {"name": "disease"}, "value": [{"name": "Not available"}]},
                {"key": {"name": "biosample accession number"}, "value": [{"value": "SAMEA12345"}]},
                {"key": {"name": "cell type"}, "value": [{"accession": "CL:0000236", "name": "B cell"}]},
            ],
            "otherOmicsLinks": [
                "pride.project:PXD001357", "px:PXD001357", "SAMEA12345",
                "biostudies:S-BSST1", "px:PXD999999", "px:PXD999999",
                "url:https://www.ncbi.nlm.nih.gov/assembly/GCF_001922835.1",
                "url:ftp://example.org/file",
                "ftp://example.org/file", "No links",
            ],
            "references": [{"pubmedID": 0, "doi": "not a doi"}],
        })
        self.assertEqual(len(node["pride:organism"]), 1)
        self.assertEqual(node["pride:organismPart"][0]["grebi:value"], "BTO:0000338")
        self.assertEqual(node["pride:disease"], ["Not available"])
        self.assertEqual(node["pride:sample"], ["biosample:SAMEA12345"])
        self.assertEqual(node["pride:cellType"][0]["grebi:value"], "CL:0000236")
        self.assertEqual(node["dcterms:references"], [
            "biosample:SAMEA12345", "biostudies:S-BSST1", "px:PXD999999",
            "https://www.ncbi.nlm.nih.gov/assembly/GCF_001922835.1",
        ])

    def test_legacy_and_affinity_accessions_have_no_proteomexchange_alias(self):
        for accession in ("PRD000001", "PAD000005"):
            with self.subTest(accession=accession):
                node = PRIDE.parse_project({"accession": accession, "doi": f"10.6019/{accession}"})
                self.assertEqual(node["id"], f"pride.project:{accession}")
                self.assertEqual(node["dcterms:identifier"], [f"doi:10.6019/{accession}"])

    def test_identifier_validation(self):
        for accession in (None, "", "PXDnope", "PXF123456", "PXD001357 extra"):
            with self.subTest(accession=accession), self.assertRaises(ValueError):
                PRIDE.parse_project({"accession": accession})
        for value in ("10.1234/example", "doi:10.1234/example", "https://doi.org/10.1234/example"):
            self.assertEqual(PRIDE.doi_identifier(value), "doi:10.1234/example")

    def test_streaming_handles_chunk_boundaries_and_unicode(self):
        projects = [
            {"accession": "PXD001357", "title": 'A long string with ],{ and "quotes" — Zürich' * 20},
            {"accession": "PAD000005"},
        ]
        for chunk_size in (1, 7, 64, 65536):
            with self.subTest(chunk_size=chunk_size):
                stream = io.StringIO(" \n" + json.dumps(projects, ensure_ascii=False) + "\n ")
                self.assertEqual(list(PRIDE.iter_projects(stream, chunk_size)), projects)

    def test_streaming_rejects_truncated_or_invalid_json(self):
        for value in ('', '{}', '[', '[{}', '[{},]', '[1]', '[{}]junk', '[{"title":"cut'):
            for chunk_size in (1, 65536):
                with self.subTest(value=value, chunk_size=chunk_size), self.assertRaises(ValueError):
                    list(PRIDE.iter_projects(io.StringIO(value), chunk_size))
        # A valid JSON object is not necessarily a valid PRIDE project.
        with self.assertRaisesRegex(ValueError, "accession"):
            PRIDE.ingest(io.StringIO('[{}]'), io.StringIO())
        for value in ('[{"accession":"PXD001357"}', '[{"accession":"PXD001357"},'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                PRIDE.ingest(io.StringIO(value), io.StringIO())

    def test_ingest_rejects_empty_and_duplicate_exports(self):
        with self.assertRaisesRegex(ValueError, "no projects"):
            PRIDE.ingest(io.StringIO("[]"), io.StringIO())
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            PRIDE.ingest(io.StringIO(json.dumps([{"accession": "PXD001357"}] * 2)), io.StringIO())

    def test_ingest_writes_one_json_node_per_line(self):
        stream = io.StringIO('[{"accession":"PXD001357"},{"accession":"PRD000001"}]')
        output = io.StringIO()
        self.assertEqual(PRIDE.ingest(stream, output), 2)
        self.assertEqual([json.loads(line)["id"] for line in output.getvalue().splitlines()], [
            "pride.project:PXD001357", "pride.project:PRD000001",
        ])


if __name__ == "__main__":
    unittest.main()
