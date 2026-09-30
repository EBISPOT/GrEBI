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


SNAPSHOT = load_module("ega_snapshot", "dataload/00_download/ega_snapshot.py")


def study(accession, released=True, deprecated=False, **rest):
    return {"accession_id": accession, "is_released": released, "is_deprecated": deprecated, **rest}


def fake_get(body):
    calls = []

    def get(url):
        calls.append(url)
        return json.dumps(body).encode()

    get.calls = calls
    return get


class ListingTest(unittest.TestCase):
    def test_every_study_comes_in_one_request(self):
        get = fake_get([study("EGAS2"), study("EGAS1")])
        studies = SNAPSHOT.list_studies("https://api", 10, get=get)
        self.assertEqual([s["accession_id"] for s in studies], ["EGAS2", "EGAS1"])
        self.assertEqual(get.calls, ["https://api/studies?limit=10"])

    def test_a_full_page_is_refused_as_it_may_be_cut_short(self):
        get = fake_get([study("EGAS1"), study("EGAS2")])
        with self.assertRaisesRegex(ValueError, "fill the page"):
            SNAPSHOT.list_studies("https://api", 2, get=get)

    def test_an_answer_that_is_not_a_list_of_studies_is_refused(self):
        with self.assertRaisesRegex(ValueError, "expected a JSON list"):
            SNAPSHOT.list_studies("https://api", 10, get=fake_get({"error": "down"}))
        with self.assertRaisesRegex(ValueError, "without an accession_id"):
            SNAPSHOT.list_studies("https://api", 10, get=fake_get([{"title": "no id"}]))


class SnapshotTest(unittest.TestCase):
    def test_only_released_current_studies_are_kept_in_accession_order(self):
        kept = SNAPSHOT.released([
            study("EGAS3"),
            study("DUMMY_STUDY", released=False),
            study("EGAS1", pubmed_ids=[21248752]),
            study("EGAS2", deprecated=True),
        ])
        self.assertEqual([s["accession_id"] for s in kept], ["EGAS1", "EGAS3"])

    def test_a_study_listed_twice_is_refused(self):
        with self.assertRaisesRegex(ValueError, "listed twice"):
            SNAPSHOT.released([study("EGAS1"), study("EGAS1")])

    def test_the_snapshot_is_one_study_a_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "ega_studies.jsonl.gz"
            written = SNAPSHOT.write_snapshot([study("EGAS1", title="é"), study("EGAS2")], output)
            self.assertEqual(written, 2)
            with gzip.open(output, "rt", encoding="utf-8") as lines:
                rows = [json.loads(line) for line in lines]
            self.assertEqual([r["accession_id"] for r in rows], ["EGAS1", "EGAS2"])
            self.assertEqual(rows[0]["title"], "é")
            self.assertFalse((Path(tmp) / "ega_studies.jsonl.gz.part").exists())


if __name__ == "__main__":
    unittest.main()
