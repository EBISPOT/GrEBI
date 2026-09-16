#!/usr/bin/env python3
"""Unit tests for the PrimeKG ingest (pure functions, no data file)."""

import json
import unittest

import primekg


class TestCurie(unittest.TestCase):
    def test_ontology_ids_are_padded_to_seven_digits(self):
        # as PrimeKG writes them: leading zeros dropped
        self.assertEqual(primekg.curie("MONDO", "5083"), "MONDO:0005083")
        self.assertEqual(primekg.curie("MONDO", "482"), "MONDO:0000482")
        self.assertEqual(primekg.curie("HPO", "855"), "HPO:0000855")
        self.assertEqual(primekg.curie("GO", "51581"), "GO:0051581")
        self.assertEqual(primekg.curie("UBERON", "2"), "UBERON:0000002")

    def test_already_seven_digits_unchanged(self):
        self.assertEqual(primekg.curie("GO", "1903891"), "GO:1903891")
        self.assertEqual(primekg.curie("UBERON", "8000004"), "UBERON:8000004")

    def test_other_sources_untouched(self):
        self.assertEqual(primekg.curie("NCBI", "9796"), "NCBIGene:9796")
        self.assertEqual(primekg.curie("DrugBank", "DB09130"), "DrugBank:DB09130")
        self.assertEqual(primekg.curie("REACTOME", "R-HSA-109581"), "REACTOME:R-HSA-109581")
        self.assertEqual(primekg.curie("MONDO_grouped", "1200_1134_15512"), "MONDO_grouped:1200_1134_15512")

    def test_ctd_exposures_are_mesh_ids(self):
        self.assertEqual(primekg.curie("CTD", "D000075182"), "MESH:D000075182")
        self.assertEqual(primekg.curie("CTD", "C092102"), "MESH:C092102")
        self.assertEqual(primekg.curie("MONDO_grouped", "1200_1134_15512"), "MONDO_grouped:1200_1134_15512")


class TestRecords(unittest.TestCase):
    def test_edge_row_yields_source_with_edge_and_bare_target(self):
        row = {"relation": "indication", "display_relation": "indication", "x_index": "1",
               "x_id": "DB00001", "x_type": "drug", "x_name": "Lepirudin", "x_source": "DrugBank",
               "y_index": "2", "y_id": "5083", "y_type": "disease", "y_name": "psoriasis", "y_source": "MONDO"}
        src, tgt = list(primekg.records(row))
        self.assertEqual(src["id"], "DrugBank:DB00001")
        self.assertEqual(src["primekg:indication"]["grebi:value"], "MONDO:0005083")
        self.assertEqual(src["primekg:indication"]["grebi:properties"]["primekg:y_id"], ["5083"])
        self.assertEqual(tgt, {"id": "MONDO:0005083", "grebi:name": "psoriasis", "grebi:type": "biolink:Entity"})
        json.dumps(src); json.dumps(tgt)


if __name__ == "__main__":
    unittest.main()
