#!/usr/bin/env python3
"""Unit tests for grebi_materialise (pure string transforms — no Neo4j)."""

import unittest
import grebi_materialise as gm


class TestClassification(unittest.TestCase):
    def test_standalone(self):
        t = {"title": "x", "result_columns": [{"column_id": "a", "column_type": "string"}],
             "materialise": {"cypher": "RETURN 1 AS a"}}
        self.assertTrue(gm.is_materialised(t))
        self.assertTrue(gm.is_standalone(t))
        self.assertFalse(gm.is_parameterised(t))
        self.assertEqual(gm.query_to_run(t), "RETURN 1 AS a")

    def test_standalone_requires_result_columns(self):
        t = {"title": "x", "materialise": {"cypher": "RETURN 1 AS a"}}
        with self.assertRaises(ValueError):
            gm.query_to_run(t)

    def test_live_only(self):
        t = {"title": "x", "cypher_match_fragment": "MATCH (n)"}
        self.assertFalse(gm.is_materialised(t))

    def test_materialise_false_is_live(self):
        # documented opt-out: `materialise: false` (or any non-mapping) means live
        self.assertFalse(gm.is_materialised({"params": [{"param_id": "x"}], "materialise": False}))
        self.assertFalse(gm.is_materialised({"materialise": True}))
        self.assertFalse(gm.is_materialised({"materialise": None}))
        self.assertFalse(gm.is_materialised({}))
        # and none of these crash the classifiers
        self.assertFalse(gm.is_standalone({"materialise": True}))
        self.assertFalse(gm.is_parameterised({"materialise": False}))

    def test_parameterised(self):
        t = {
            "params": [{"param_id": "c_id", "param_type": "SourceId"}],
            "materialise": {},
            "cypher_match_fragment": "MATCH (c)-[:`biolink:broad_match`*0..1]->(x)-[:sourceId]->(:Id {id: $c_id})",
            "cypher_return_fragment": "RETURN DISTINCT c AS c",
        }
        self.assertTrue(gm.is_parameterised(t))
        self.assertFalse(gm.is_standalone(t))


class TestFiltersColumn(unittest.TestCase):
    def test_derived_from_param_id(self):
        self.assertEqual(gm.filters_column({"param_id": "cell_type_id"}), "cell_type")

    def test_param_id_must_end_in_id(self):
        with self.assertRaises(ValueError):
            gm.filters_column({"param_id": "cell_type"})
        with self.assertRaises(ValueError):
            gm.filters_column({"param_id": "_id"})


class TestDerivedMatches(unittest.TestCase):
    def test_closure_root_anchor_means_descendants(self):
        t = {
            "cypher_match_fragment":
                "MATCH (cell_type)-[:`biolink:broad_match`*0..1]->(x)-[:sourceId]->(:Id {id: $cell_type_id })",
            "cypher_return_fragment": "RETURN DISTINCT cell_type { .id } AS cell_type",
        }
        p = {"param_id": "cell_type_id", "param_type": "SourceId"}
        self.assertEqual(gm.derived_matches(t, p), "descendants")

    def test_bare_anchor_means_exact(self):
        t = {
            "cypher_match_fragment": "MATCH (disease)-[:sourceId]->(:Id {id: $disease_id})",
            "cypher_return_fragment": "RETURN DISTINCT disease { .id } AS disease",
        }
        p = {"param_id": "disease_id", "param_type": "SourceId"}
        self.assertEqual(gm.derived_matches(t, p), "exact")

    def test_missing_anchor_raises(self):
        t = {"cypher_match_fragment": "MATCH (n)", "cypher_return_fragment": "RETURN n AS disease"}
        with self.assertRaises(ValueError):
            gm.derived_matches(t, {"param_id": "disease_id", "param_type": "SourceId"})

    def test_non_canonical_rollup_rejected(self):
        # old style: anchor binds `disease` exactly, but a separate reversed
        # broad_match roll-up produces the variable actually returned AS disease
        # — anchor shape (exact) would not describe the rows (descendants).
        t = {
            "cypher_match_fragment":
                "MATCH (disease)-[:sourceId]->(:Id {id: $disease_id})\n"
                "MATCH (disease)<-[:`biolink:broad_match`*0..1]-(specific_disease)\n"
                "MATCH (specific_disease)-[:`biolink:has_phenotype`]->(phenotype)",
            "cypher_return_fragment":
                "RETURN DISTINCT specific_disease { .id } AS disease, phenotype { .id } AS phenotype",
        }
        with self.assertRaises(ValueError):
            gm.derived_matches(t, {"param_id": "disease_id", "param_type": "SourceId"})

    def test_rollup_feeding_another_column_is_fine(self):
        # a reversed broad_match off the anchored var is legitimate when it
        # feeds a DIFFERENT result column (gwas_by_gene_and_disease's trait)
        t = {
            "cypher_match_fragment":
                "MATCH (disease)-[:sourceId]->(:Id {id: $disease_id})\n"
                "MATCH (disease)<-[:`biolink:broad_match`*0..1]-(trait)\n"
                "MATCH (snp)-[:`gwas:associated_with`]->(trait)",
            "cypher_return_fragment":
                "RETURN DISTINCT disease { .id } AS disease, trait { .id } AS trait",
        }
        p = {"param_id": "disease_id", "param_type": "SourceId"}
        self.assertEqual(gm.derived_matches(t, p), "exact")


class TestValuesUnderClosureRootSubstitution(unittest.TestCase):
    def test_cell_type_root_substitution(self):
        t = {
            "params": [{"param_id": "cell_type_id", "param_type": "SourceId",
                        "values_under": "cl:0000000"}],
            "materialise": {},
            "cypher_match_fragment":
                "MATCH (cell_type)-[:`biolink:broad_match`*0..1]->(x)-[:sourceId]->(:Id {id: $cell_type_id })\n"
                "MATCH (cell_type)<-[r]-(matched_trait)",
            "cypher_return_fragment": "RETURN DISTINCT cell_type { .id } AS cell_type",
        }
        q = gm.derive_materialise_query(t)
        self.assertIn(":Id {id: 'cl:0000000' }", q)
        self.assertNotIn("$cell_type_id", q)
        self.assertIn("RETURN DISTINCT cell_type { .id } AS cell_type", q)


class TestValuesUnderBareAnchorWrap(unittest.TestCase):
    def test_disease_exact_wraps_anchor(self):
        t = {
            "params": [{"param_id": "disease_id", "param_type": "SourceId",
                        "values_under": "mondo:0000001"}],
            "materialise": {},
            "cypher_match_fragment":
                "MATCH (disease)-[:sourceId]->(:Id {id: $disease_id})\n"
                "MATCH (disease)-[r:`biolink:affects`]->(process)",
            "cypher_return_fragment": "RETURN DISTINCT disease { .id } AS disease, process AS process",
        }
        q = gm.derive_materialise_query(t)
        self.assertIn("-[:`biolink:broad_match`*0..1]->(__disease_id_dom)-[:sourceId]->(:Id {id: 'mondo:0000001'})", q)
        self.assertNotIn("$disease_id", q)
        # base var still present and unlabelled-anchored via the wrap
        self.assertIn("MATCH (disease)-[:`biolink:broad_match`", q)


class TestValuesWithTypeAnchorDrop(unittest.TestCase):
    def test_gene_label_drops_anchor(self):
        t = {
            "params": [{"param_id": "gene_id", "param_type": "SourceId",
                        "values_with_type": "hgnc:Gene"}],
            "materialise": {},
            "cypher_match_fragment":
                "MATCH (gene:`hgnc:Gene`)-[:sourceId]->(:Id {id: $gene_id })\n"
                "MATCH (gene)<-[:`gwas:mapped_gene`]-(snp)",
            "cypher_return_fragment": "RETURN DISTINCT gene { .id } AS gene",
        }
        q = gm.derive_materialise_query(t)
        self.assertIn("MATCH (gene:`hgnc:Gene`)\n", q)
        self.assertNotIn("sourceId", q.split("\n")[0])
        self.assertNotIn("$gene_id", q)

    def test_label_must_match_declared_type(self):
        t = {
            "params": [{"param_id": "gene_id", "param_type": "SourceId",
                        "values_with_type": "hgnc:Gene"}],
            "materialise": {},
            "cypher_match_fragment": "MATCH (gene)-[:sourceId]->(:Id {id: $gene_id })",
            "cypher_return_fragment": "RETURN DISTINCT gene { .id } AS gene",
        }
        with self.assertRaises(ValueError):
            gm.derive_materialise_query(t)

    def test_closure_root_anchor_drops_whole_hop(self):
        # chebi_to_metabolights: values_with_type + closure-root anchor — the
        # drop must take the hop with it, leaving just the labelled base.
        t = {
            "params": [{"param_id": "chemical_id", "param_type": "SourceId",
                        "values_with_type": "biolink:ChemicalEntity"}],
            "materialise": {},
            "cypher_match_fragment":
                "MATCH (chemical:`biolink:ChemicalEntity`)-[:`biolink:broad_match`*0..1]->(chemical_root)-[:sourceId]->(:Id { id: $chemical_id })\n"
                "MATCH (study:`metabolights:Study`)-[:`metabolights:ref`]->(chemical)",
            "cypher_return_fragment": "RETURN DISTINCT chemical { .id } AS chemical, study { .id } AS study",
        }
        q = gm.derive_materialise_query(t)
        self.assertIn("MATCH (chemical:`biolink:ChemicalEntity`)\n", q)
        self.assertNotIn("broad_match", q.split("RETURN")[0])
        self.assertNotIn("$chemical_id", q)

    def test_hop_var_referenced_elsewhere_rejected(self):
        t = {
            "params": [{"param_id": "chemical_id", "param_type": "SourceId",
                        "values_with_type": "biolink:ChemicalEntity"}],
            "materialise": {},
            "cypher_match_fragment":
                "MATCH (chemical:`biolink:ChemicalEntity`)-[:`biolink:broad_match`*0..1]->(chemical_root)-[:sourceId]->(:Id { id: $chemical_id })\n"
                "MATCH (chemical_root)-[:`x:y`]->(other)",
            "cypher_return_fragment": "RETURN DISTINCT chemical { .id } AS chemical",
        }
        with self.assertRaises(ValueError):
            gm.derive_materialise_query(t)


class TestUnconstrainedAnchorDrop(unittest.TestCase):
    def test_no_values_drops_bare_anchor(self):
        # phenotype_to_diseases: unconstrained param, bare anchor
        t = {
            "params": [{"param_id": "phenotype_id", "param_type": "SourceId"}],
            "materialise": {},
            "cypher_match_fragment":
                "MATCH (phenotype)-[:sourceId]->(:Id {id: $phenotype_id})\n"
                "MATCH (disease)-[r:`biolink:has_phenotype`]->(phenotype)",
            "cypher_return_fragment": "RETURN DISTINCT disease { .id } AS disease, phenotype { .id } AS phenotype",
        }
        q = gm.derive_materialise_query(t)
        self.assertIn("MATCH (phenotype)\n", q)
        self.assertNotIn("$phenotype_id", q)

    def test_no_values_drops_closure_root_anchor(self):
        # gwas_traits_reported_different_from_matched: unconstrained, hop anchor
        t = {
            "params": [{"param_id": "trait_id", "param_type": "SourceId"}],
            "materialise": {},
            "cypher_match_fragment":
                "MATCH (snp)-[assoc:`gwas:associated_with`]->(trait)\n"
                "MATCH (trait)-[:`biolink:broad_match`*0..1]->(parent_term)-[:sourceId]->(:Id { id: $trait_id })",
            "cypher_return_fragment": "RETURN DISTINCT trait { .id } AS trait",
        }
        q = gm.derive_materialise_query(t)
        self.assertIn("MATCH (trait)\n", q)
        self.assertNotIn("parent_term", q)
        self.assertNotIn("$trait_id", q)
        # ...but its serving closure still comes from the (hop) anchor shape
        self.assertEqual(gm.serving_metadata(t)["params"][0]["closure"], "descendants")


class TestValuesMutuallyExclusive(unittest.TestCase):
    def test_both_values_fields_rejected(self):
        t = {
            "params": [{"param_id": "x_id", "param_type": "SourceId",
                        "values_under": "a:1", "values_with_type": "b:T"}],
            "materialise": {},
            "cypher_match_fragment": "MATCH (x)-[:sourceId]->(:Id {id: $x_id})",
            "cypher_return_fragment": "RETURN DISTINCT x { .id } AS x",
        }
        with self.assertRaises(ValueError):
            gm.derive_materialise_query(t)


class TestMixedParams(unittest.TestCase):
    def test_gene_label_and_cell_type_id(self):
        t = {
            "params": [
                {"param_id": "gene_id", "param_type": "SourceId",
                 "values_with_type": "hgnc:Gene"},
                {"param_id": "cell_type_id", "param_type": "SourceId",
                 "values_under": "cl:0000000"},
            ],
            "materialise": {},
            "cypher_match_fragment":
                "MATCH (gene:`hgnc:Gene`)-[:sourceId]->(:Id {id: $gene_id })\n"
                "MATCH (cell_type)-[:`biolink:broad_match`*0..1]->(x)-[:sourceId]->(:Id {id: $cell_type_id })",
            "cypher_return_fragment": "RETURN DISTINCT gene { .id } AS gene, cell_type { .id } AS cell_type",
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
                {"param_id": "mouse_gene_id", "param_type": "SourceId",
                 "values_with_type": "impc:MouseGene"},
                {"param_id": "min_score", "param_type": "float", "param_default": "0.9"},
            ],
            "materialise": {},
            "cypher_match_fragment":
                "MATCH (mouse_gene:`impc:MouseGene`)-[:sourceId]->(:Id {id: $mouse_gene_id })\n"
                "WHERE toFloat(evidence.`otar:score`[0]) >= $min_score",
            "cypher_return_fragment": "RETURN DISTINCT mouse_gene { .id } AS mouse_gene",
        }
        q = gm.derive_materialise_query(t)
        self.assertIn(">= 0.9", q)
        self.assertNotIn("$min_score", q)
        self.assertNotIn("$mouse_gene_id", q)

    def test_required_param_without_default_raises(self):
        t = {
            "params": [
                {"param_id": "base_id", "param_type": "SourceId", "values_under": "x:0"},
                {"param_id": "other", "param_type": "string"},  # no default, not SourceId
            ],
            "materialise": {},
            "cypher_match_fragment":
                "MATCH (base)-[:`biolink:broad_match`*0..1]->(x)-[:sourceId]->(:Id {id: $base_id})\nWHERE n.p = $other",
            "cypher_return_fragment": "RETURN DISTINCT base { .id } AS base",
        }
        with self.assertRaises(ValueError):
            gm.derive_materialise_query(t)


class TestCountsOnly(unittest.TestCase):
    def test_counts_only_histogram(self):
        t = {
            "params": [{"param_id": "gene_id", "param_type": "SourceId",
                        "values_with_type": "hgnc:Gene"}],
            "materialise": {"mode": "counts_only"},
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


class TestValidation(unittest.TestCase):
    def _base(self):
        return {
            "params": [{"param_id": "b_id", "param_type": "SourceId", "values_under": "x:0"}],
            "materialise": {},
            "cypher_match_fragment": "MATCH (b)-[:`biolink:broad_match`*0..1]->(x)-[:sourceId]->(:Id {id: $b_id})",
            "cypher_return_fragment": "RETURN DISTINCT b { .id } AS b",
            "result_columns": [{"column_id": "b", "column_type": "GraphNodeId"}],
        }

    def test_valid_template_passes(self):
        gm.derive_materialise_query(self._base())

    def test_old_materialise_params_rejected(self):
        t = self._base()
        t["materialise"] = {"params": [{"param_id": "b_id", "filters_column": "b"}]}
        with self.assertRaises(ValueError):
            gm.derive_materialise_query(t)

    def test_derived_filters_column_must_be_result_column(self):
        t = self._base()
        t["params"][0]["param_id"] = "not_a_col_id"
        t["cypher_match_fragment"] = t["cypher_match_fragment"].replace("$b_id", "$not_a_col_id")
        with self.assertRaises(ValueError):
            gm.derive_materialise_query(t)

    def test_needs_a_sourceid_param(self):
        t = self._base()
        t["params"] = [{"param_id": "n", "param_type": "float", "param_default": "1"}]
        with self.assertRaises(ValueError):
            gm.derive_materialise_query(t)

    def test_leftover_param_detected(self):
        # a dropped anchor whose $p also appears in a WHERE would be left unbound
        t = {
            "params": [{"param_id": "b_id", "param_type": "SourceId"}],
            "cypher_match_fragment":
                "MATCH (b:`Lbl`)-[:sourceId]->(:Id {id: $b_id})\nWHERE b.foo = $b_id",
            "cypher_return_fragment": "RETURN DISTINCT b { .id } AS b",
            "result_columns": [{"column_id": "b", "column_type": "GraphNodeId"}],
            "materialise": {},
        }
        with self.assertRaises(ValueError):
            gm.derive_materialise_query(t)


class TestServingMetadata(unittest.TestCase):
    def test_parameterised_metadata_is_derived(self):
        t = {
            "params": [{"param_id": "cell_type_id", "param_type": "SourceId",
                        "values_under": "cl:0000000"}],
            "materialise": {},
            "cypher_match_fragment":
                "MATCH (cell_type)-[:`biolink:broad_match`*0..1]->(x)-[:sourceId]->(:Id {id: $cell_type_id})",
            "cypher_return_fragment": "RETURN DISTINCT cell_type { .id } AS cell_type",
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
