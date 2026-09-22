#!/usr/bin/env python3

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


BIOSTUDIES = load_module("biostudies_download", "dataload/00_download/biostudies.py")


def fake_search(pages):
    """A search() over canned results: pages[(collection or query)] -> list of accessions."""
    calls = []

    def search(api, params):
        calls.append(params)
        key = params.get("collection") or params["query"]
        accessions = pages[key]
        size, page = params["pageSize"], params["page"]
        chunk = accessions[(page - 1) * size:page * size]
        return {"totalHits": len(accessions), "hits": [{"accession": a, "type": "study"} for a in chunk]}

    search.calls = calls
    return search


class EnumerationTest(unittest.TestCase):
    def test_pages_through_collections_and_prefixes_and_deduplicates(self):
        search = fake_search({
            "BioImages": ["S-BIAD1", "S-BIAD2", "S-BSST9"],
            "S-BSST*": ["S-BSST9", "S-BSST10", "S-EPMC1234", "S-BIAD2", "E-MTAB-1"],
        })
        accessions = BIOSTUDIES.enumerate_accessions("api", ["BioImages"], ["S-BSST"], page_size=2, fetch=search)
        self.assertEqual(accessions, ["S-BIAD1", "S-BIAD2", "S-BSST10", "S-BSST9"])
        # 3 hits at page size 2 -> 2 pages; 5 hits -> 3 pages
        self.assertEqual([c["page"] for c in search.calls], [1, 2, 1, 2, 3])

    def test_prefix_query_is_free_text_so_only_real_prefix_matches_count(self):
        search = fake_search({"S-CMO*": ["S-CMO1", "S-EPMC77", "S-BSST3"]})
        self.assertEqual(BIOSTUDIES.enumerate_accessions("api", [], ["S-CMO"], fetch=search), ["S-CMO1"])

    def test_europe_pmc_is_never_included(self):
        search = fake_search({"ArrayExpress": ["E-MTAB-2", "S-EPMC5"]})
        self.assertEqual(BIOSTUDIES.enumerate_accessions("api", ["ArrayExpress"], [], fetch=search), ["E-MTAB-2"])

    def test_rejects_ids_that_are_not_accessions(self):
        search = fake_search({"X": ["../../etc", "S-BSST1", "S-BSST1;rm"]})
        self.assertEqual(BIOSTUDIES.enumerate_accessions("api", ["X"], [], fetch=search), ["S-BSST1"])


class FetchTest(unittest.TestCase):
    def test_writes_ftp_layout_keeps_existing_and_tolerates_withdrawn(self):
        served = {"api/studies/S-BSST1": b'{"accno": "S-BSST1"}', "api/studies/S-BSST3": b'{"accno": "S-BSST3"}'}
        requested = []

        def get(url):
            requested.append(url)
            return served.get(url)

        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp)
            existing = BIOSTUDIES.study_path(dest, "S-BSST3")
            existing.parent.mkdir(parents=True)
            existing.write_text('{"accno": "S-BSST3", "cached": true}')

            counts = BIOSTUDIES.fetch_all("api", dest, ["S-BSST1", "S-BSST2", "S-BSST3"], concurrency=2, get=get)

            self.assertEqual(counts, {"kept": 1, "fetched": 1, "missing": 1, "failed": 0})
            self.assertEqual(json.loads(BIOSTUDIES.study_path(dest, "S-BSST1").read_text()), {"accno": "S-BSST1"})
            self.assertTrue(json.loads(existing.read_text())["cached"], "an existing file is not refetched")
            self.assertNotIn("api/studies/S-BSST3", requested)
            self.assertFalse(BIOSTUDIES.study_path(dest, "S-BSST2").exists())
            self.assertEqual(list(dest.rglob("*.part")), [])

    def test_a_non_json_response_is_a_failure_not_a_study(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp)
            counts = BIOSTUDIES.fetch_all("api", dest, ["S-BSST1"], concurrency=1, get=lambda url: b"<html>maintenance</html>")
            self.assertEqual(counts["failed"], 1)
            self.assertFalse(BIOSTUDIES.study_path(dest, "S-BSST1").exists())

    def test_prune_removes_only_study_directories_no_longer_listed(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp)
            for accession in ("S-BSST1", "S-BSST2"):
                path = BIOSTUDIES.study_path(dest, accession)
                path.parent.mkdir(parents=True)
                path.write_text("{}")
            (dest / "notes.txt").write_text("not a study")

            self.assertEqual(BIOSTUDIES.prune(dest, ["S-BSST1"]), 1)
            self.assertTrue(BIOSTUDIES.study_path(dest, "S-BSST1").exists())
            self.assertFalse((dest / "S-BSST2").exists())
            self.assertTrue((dest / "notes.txt").exists())


if __name__ == "__main__":
    unittest.main()
