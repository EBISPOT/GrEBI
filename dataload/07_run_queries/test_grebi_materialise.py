#!/usr/bin/env python3
"""Unit tests for grebi_materialise (pure string transforms — no Neo4j)."""

import unittest
import grebi_materialise as gm


class TestClassification(unittest.TestCase):
    def test_standalone(self):
        t = {"title": "x", "materialise": {"cypher": "RETURN 1 AS a"}}
        self.assertTrue(gm.is_materialised(t))
        self.assertTrue(gm.is_standalone(t))
        self.assertFalse(gm.is_parameterised(t))
        self.assertEqual(gm.query_to_run(t), "RETURN 1 AS a")

    def test_live_only(self):
        t = {"title": "x", "cypher_match_fragment": "MATCH (n)"}
        self.assertFalse(gm.is_materialised(t))

    def test_parameterised(self):
        t = {
            "params": [{"param_id": "x", "param_type": "SourceId"}],
            "materialise": {"params": [{"param_id": "x", "filters_column": "c",
                                        "closure": "descendants", "domain_kind": "id",
                                        "domain_root": "cl:0000000"}]},
            "cypher_match_fragment": "MATCH (c)-[:`biolink:broad_match`*0..1]->(x)-[:sourceId]->(:Id {id: $x})",
            "cypher_return_fragment": "RETURN DISTINCT c AS c",
        }
        self.assertTrue(gm.is_parameterised(t))
        self.assertFalse(gm.is_standalone(t))


class TestDescendantsIdSubstitution(unittest.TestCase):
    def test_cell_type_root_substitution(self):
        t = {
            "params": [{"param_id": "cell_type_id", "param_type": "SourceId"}],
            "materialise": {"params": [{
                "param_id": "cell_type_id", "filters_column": "cell_type",
                "closure": "descendants", "domain_kind": "id",
                "domain_root": "cl:0000000"}]},
            "cypher_match_fragment":
                "MATCH (cl_term)-[:`biolink:broad_match`*0..1]->(x)-[:sourceId]->(:Id {id: $cell_type_id })\n"
                "MATCH (cl_term)<-[r]-(matched_trait)",
            "cypher_return_fragment": "RETURN DISTINCT cl_term AS cell_type",
        }
        q = gm.derive_materialise_query(t)
        self.assertIn(":Id {id: 'cl:0000000' }", q)
        self.assertNotIn("$cell_type_id", q)
        self.assertIn("RETURN DISTINCT cl_term AS cell_type", q)


class TestExactIdWrap(unittest.TestCase):
    def test_disease_exact_wraps_anchor(self):
        t = {
            "params": [{"param_id": "disease_id", "param_type": "SourceId"}],
            "materialise": {"params": [{
                "param_id": "disease_id", "filters_column": "disease",
                "closure": "exact", "domain_kind": "id",
                "domain_root": "mondo:0000001"}]},
            "cypher_match_fragment":
                "MATCH (disease)-[:sourceId]->(:Id {id: $disease_id})\n"
                "MATCH (disease)-[r:`biolink:affects`]->(process)",
            "cypher_return_fragment": "RETURN DISTINCT disease AS disease, process AS process",
        }
        q = gm.derive_materialise_query(t)
        self.assertIn("-[:`biolink:broad_match`*0..1]->(__disease_id_dom)-[:sourceId]->(:Id {id: 'mondo:0000001'})", q)
        self.assertNotIn("$disease_id", q)
        # base var still present and unlabelled-anchored via the wrap
        self.assertIn("MATCH (disease)-[:`biolink:broad_match`", q)


class TestLabelAnchorDrop(unittest.TestCase):
    def test_gene_label_drops_anchor(self):
        t = {
            "params": [{"param_id": "gene_id", "param_type": "SourceId"}],
            "materialise": {"params": [{
                "param_id": "gene_id", "filters_column": "gene",
                "closure": "exact", "domain_kind": "label",
                "domain_root": "hgnc:Gene"}]},
            "cypher_match_fragment":
                "MATCH (gene:`hgnc:Gene`)-[:sourceId]->(:Id {id: $gene_id })\n"
                "MATCH (gene)<-[:`gwas:mapped_gene`]-(snp)",
            "cypher_return_fragment": "RETURN DISTINCT gene AS gene",
        }
        q = gm.derive_materialise_query(t)
        self.assertIn("MATCH (gene:`hgnc:Gene`)\n", q)
        self.assertNotIn("sourceId", q.split("\n")[0])
        self.assertNotIn("$gene_id", q)


class TestMixedParams(unittest.TestCase):
    def test_gene_label_and_cell_type_id(self):
        t = {
            "params": [
                {"param_id": "gene_id", "param_type": "SourceId"},
                {"param_id": "cell_type_id", "param_type": "SourceId"},
            ],
            "materialise": {"params": [
                {"param_id": "gene_id", "filters_column": "gene", "closure": "exact",
                 "domain_kind": "label", "domain_root": "hgnc:Gene"},
                {"param_id": "cell_type_id", "filters_column": "cell_type",
                 "closure": "descendants", "domain_kind": "id", "domain_root": "cl:0000000"},
            ]},
            "cypher_match_fragment":
                "MATCH (gene:`hgnc:Gene`)-[:sourceId]->(:Id {id: $gene_id })\n"
                "MATCH (cl_term)-[:`biolink:broad_match`*0..1]->(x)-[:sourceId]->(:Id {id: $cell_type_id })",
            "cypher_return_fragment": "RETURN DISTINCT gene AS gene, cl_term AS cell_type",
        }
        q = gm.derive_materialise_query(t)
        self.assertNotIn("$gene_id", q)
        self.assertNotIn("$cell_type_id", q)
        self.assertIn("MATCH (gene:`hgnc:Gene`)\n", q)
        self.assertIn(":Id {id: 'cl:0000000' }", q)


class TestValueParamDefaults(unittest.TestCase):
    def test_float_default_substituted(self):
        t = {
            "params": [
                {"param_id": "mouse_gene_id", "param_type": "SourceId"},
                {"param_id": "min_score", "param_type": "float", "param_default": "0.9"},
            ],
            "materialise": {"params": [
                {"param_id": "mouse_gene_id", "filters_column": "mouse_gene",
                 "closure": "exact", "domain_kind": "label", "domain_root": "impc:MouseGene"},
            ]},
            "cypher_match_fragment":
                "MATCH (mouse_gene:`impc:MouseGene`)-[:sourceId]->(:Id {id: $mouse_gene_id })\n"
                "WHERE toFloat(evidence.`otar:score`[0]) >= $min_score",
            "cypher_return_fragment": "RETURN DISTINCT mouse_gene AS mouse_gene",
        }
        q = gm.derive_materialise_query(t)
        self.assertIn(">= 0.9", q)
        self.assertNotIn("$min_score", q)
        self.assertNotIn("$mouse_gene_id", q)

    def test_required_param_without_default_raises(self):
        t = {
            "params": [
                {"param_id": "base_id", "param_type": "SourceId"},
                {"param_id": "other", "param_type": "string"},  # no default, not materialise
            ],
            "materialise": {"params": [
                {"param_id": "base_id", "filters_column": "b", "closure": "descendants",
                 "domain_kind": "id", "domain_root": "x:0"},
            ]},
            "cypher_match_fragment":
                "MATCH (b)-[:`biolink:broad_match`*0..1]->(x)-[:sourceId]->(:Id {id: $base_id})\nWHERE n.p = $other",
            "cypher_return_fragment": "RETURN DISTINCT b AS b",
        }
        with self.assertRaises(ValueError):
            gm.derive_materialise_query(t)


class TestCountsOnly(unittest.TestCase):
    def test_counts_only_histogram(self):
        t = {
            "params": [{"param_id": "gene_id", "param_type": "SourceId"}],
            "materialise": {"mode": "counts_only", "params": [
                {"param_id": "gene_id", "filters_column": "gene", "closure": "exact",
                 "domain_kind": "label", "domain_root": "hgnc:Gene"},
            ]},
            "cypher_match_fragment":
                "MATCH (gene:`hgnc:Gene`)-[:sourceId]->(:Id {id: $gene_id})\n"
                "MATCH (gene)-[r]->(disease)",
            "cypher_return_fragment":
                "RETURN DISTINCT gene { .id } AS gene, disease { .id } AS disease",
        }
        q = gm.derive_materialise_query(t)
        self.assertIn("WITH DISTINCT gene { .id } AS gene, disease { .id } AS disease", q)
        self.assertIn("RETURN `gene`, count(*) AS _count", q)
        self.assertNotIn("RETURN DISTINCT", q)


class TestServingMetadata(unittest.TestCase):
    def test_parameterised_metadata(self):
        t = {
            "params": [{"param_id": "cell_type_id", "param_type": "SourceId"}],
            "materialise": {"params": [{
                "param_id": "cell_type_id", "filters_column": "cell_type",
                "closure": "descendants", "domain_kind": "id", "domain_root": "cl:0000000"}]},
            "cypher_match_fragment": "MATCH (x)",
            "cypher_return_fragment": "RETURN DISTINCT x AS cell_type",
        }
        md = gm.serving_metadata(t)
        self.assertEqual(md["kind"], "parameterised")
        self.assertEqual(md["mode"], "full")
        self.assertEqual(md["params"][0]["filters_column"], "cell_type")
        self.assertEqual(md["params"][0]["closure"], "descendants")
        self.assertEqual(md["params"][0]["param_type"], "SourceId")

    def test_standalone_metadata(self):
        t = {"materialise": {"cypher": "RETURN 1 AS a"}}
        self.assertEqual(gm.serving_metadata(t)["kind"], "standalone")


class TestRunsForSubgraph(unittest.TestCase):
    def test_run_for_subgraphs_allowlist(self):
        t = {"materialise": {"cypher": "RETURN 1", "run_for_subgraphs": ["impc_x_gwas"]}}
        self.assertTrue(gm.runs_for_subgraph(t, "impc_x_gwas"))
        self.assertFalse(gm.runs_for_subgraph(t, "other"))

    def test_graphs_fallback(self):
        t = {"graphs": ["a", "b"], "materialise": {"params": [{"param_id": "p"}]}}
        self.assertTrue(gm.runs_for_subgraph(t, "a"))
        self.assertFalse(gm.runs_for_subgraph(t, "c"))

    def test_default_all(self):
        t = {"materialise": {"cypher": "RETURN 1"}}
        self.assertTrue(gm.runs_for_subgraph(t, "anything"))


if __name__ == "__main__":
    unittest.main()
