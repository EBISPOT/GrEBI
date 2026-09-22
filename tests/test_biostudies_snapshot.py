#!/usr/bin/env python3

import gzip
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


SNAPSHOT = load_module("biostudies_snapshot", "dataload/00_download/biostudies_snapshot.py")
RESULT_WINDOW = SNAPSHOT.RESULT_WINDOW


def fake_search(pages, years=None):
    """A search() over canned results: pages[collection or query] -> accessions.

    With `years`, a dict accession -> release year, `facet.released_year`
    narrows the result like the real API does.
    """
    calls = []

    def search(api, params):
        calls.append(params)
        accessions = pages[params.get("collection") or params["query"]]
        if "facet.released_year" in params:
            accessions = [a for a in accessions if str(years[a]) == params["facet.released_year"]]
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
        accessions = SNAPSHOT.enumerate_accessions("api", ["BioImages"], ["S-BSST"], page_size=2, fetch=search)
        self.assertEqual(accessions, ["S-BIAD1", "S-BIAD2", "S-BSST10", "S-BSST9"])
        self.assertEqual([c["page"] for c in search.calls], [1, 2, 1, 2, 3])

    def test_a_query_over_the_result_window_is_sliced_by_release_year(self):
        big = [f"E-MTAB-{i}" for i in range(RESULT_WINDOW + 5)]
        years = {a: 2010 + (i % 3) for i, a in enumerate(big)}
        search = fake_search({"ArrayExpress": big}, years)
        accessions = SNAPSHOT.enumerate_accessions("api", ["ArrayExpress"], [], page_size=10000, fetch=search)
        self.assertEqual(len(accessions), len(big))
        self.assertEqual(sorted(accessions), accessions)
        # one probe of the whole collection, then per-year queries only
        self.assertNotIn("facet.released_year", search.calls[0])
        self.assertTrue(all("facet.released_year" in c for c in search.calls[1:]))
        self.assertLessEqual(max(int(c["page"]) for c in search.calls), 1)  # each year fits one 10k page

    def test_prefix_query_is_free_text_so_only_real_prefix_matches_count(self):
        search = fake_search({"S-CMO*": ["S-CMO1", "S-EPMC77", "S-BSST3"]})
        self.assertEqual(SNAPSHOT.enumerate_accessions("api", [], ["S-CMO"], fetch=search), ["S-CMO1"])

    def test_europe_pmc_and_non_accessions_are_never_included(self):
        search = fake_search({"ArrayExpress": ["E-MTAB-2", "S-EPMC5", "../../etc", "S-BSST1;rm"]})
        self.assertEqual(SNAPSHOT.enumerate_accessions("api", ["ArrayExpress"], [], fetch=search), ["E-MTAB-2"])


class SnapshotTest(unittest.TestCase):
    def test_fetch_resumes_and_snapshot_is_one_document_per_line_in_order(self):
        served = {"api/studies/S-BSST1": b'{"accno": "S-BSST1"}', "api/studies/E-MTAB-1": b'{"accno":\n "E-MTAB-1"}'}
        requested = []

        def get(url):
            requested.append(url)
            return served.get(url)

        with tempfile.TemporaryDirectory() as tmp:
            scratch = Path(tmp) / "scratch"
            scratch.mkdir()
            (scratch / "S-BSST3.json").write_text('{"accno": "S-BSST3", "cached": true}')
            accessions = ["E-MTAB-1", "S-BSST1", "S-BSST2", "S-BSST3"]

            counts = SNAPSHOT.fetch_all("api", scratch, accessions, concurrency=2, get=get)
            self.assertEqual(counts, {"kept": 1, "fetched": 2, "missing": 1, "failed": 0})
            self.assertNotIn("api/studies/S-BSST3", requested)
            self.assertEqual(list(scratch.glob("*.part")), [])

            output = Path(tmp) / "biostudies.jsonl.gz"
            self.assertEqual(SNAPSHOT.write_snapshot(scratch, accessions, output), 3)
            lines = gzip.open(output, "rt").read().splitlines()
            self.assertEqual([json.loads(l)["accno"] for l in lines], ["E-MTAB-1", "S-BSST1", "S-BSST3"])
            self.assertEqual(lines[0], '{"accno":"E-MTAB-1"}')  # compact, one line per document

    def test_a_non_json_or_non_object_response_is_a_failure_not_a_study(self):
        with tempfile.TemporaryDirectory() as tmp:
            scratch = Path(tmp)
            counts = SNAPSHOT.fetch_all("api", scratch, ["S-BSST1", "S-BSST2"], concurrency=1,
                                        get=lambda url: b"<html>maintenance</html>" if "S-BSST1" in url else b"[1, 2]")
            self.assertEqual(counts["failed"], 2)
            self.assertEqual(list(scratch.glob("*.json")), [])


if __name__ == "__main__":
    unittest.main()
