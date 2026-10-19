package uk.ac.ebi.grebi;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import uk.ac.ebi.grebi.db.GrebiPostgresClient;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The node queries that identifiers drive, against a live Postgres: the bulk
 * lookup, and the listings and searches whose totals must be counts and whose
 * pages must hold what the totals say (#87, #88, #89). The table carries the
 * indexes the dataload makes, so the SQL is checked against them. Skipped
 * unless GREBI_TEST_POSTGRES=true, as MaterialisedClosureServingTest is.
 */
class NodeIdentifierQueriesPostgresTest {

    static final String GRAPH = "itnodes";
    static final String TABLE = "\"nodes_" + GRAPH + "\"";
    static GrebiPostgresClient pg;

    static boolean enabled() {
        return "true".equalsIgnoreCase(System.getenv("GREBI_TEST_POSTGRES"));
    }

    @BeforeAll
    static void setup() throws Exception {
        assumeTrue(enabled(), "GREBI_TEST_POSTGRES not set; skipping Postgres integration test");
        pg = new GrebiPostgresClient();
        try (Connection conn = pg.getConnection(); Statement st = conn.createStatement()) {
            st.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm");
            st.execute("DROP TABLE IF EXISTS " + TABLE);
            st.execute("CREATE TABLE " + TABLE + " (\"grebi:nodeId\" TEXT, \"grebi:name\" TEXT, \"grebi:type\" TEXT[],"
                    + " \"grebi:datasources\" TEXT[], \"grebi:sourceIds\" TEXT[], \"grebi:curie\" TEXT, \"ols:curie\" TEXT)");
            st.execute("CREATE INDEX ON " + TABLE + " USING btree (\"grebi:nodeId\")");
            st.execute("CREATE INDEX ON " + TABLE + " USING btree (\"grebi:curie\")");
            st.execute("CREATE INDEX ON " + TABLE + " USING btree (\"ols:curie\")");
            st.execute("CREATE INDEX ON " + TABLE + " USING btree (\"grebi:name\")");
            st.execute("CREATE INDEX ON " + TABLE + " USING gin (\"grebi:name\" gin_trgm_ops)");
            st.execute("CREATE INDEX ON " + TABLE + " USING gin (\"grebi:type\")");
            st.execute("CREATE INDEX ON " + TABLE + " USING gin (\"grebi:datasources\")");
            st.execute("CREATE INDEX ON " + TABLE + " USING gin (\"grebi:sourceIds\")");
            // A GWAS study and a Metabolights study, named by their titles; a
            // disease merged with a gene, under the gene's id; two EFO terms;
            // and a taxon whose canonical id is its biolink name.
            st.execute("INSERT INTO " + TABLE + " VALUES "
                    + "('GCST1', 'Efficiently controlling for case-control imbalance', ARRAY['gwas:Study'], ARRAY['GWAS'], ARRAY['GCST1'], 'GCST1', NULL),"
                    + "('MTBLS1', 'A metabolomic study of urinary changes', ARRAY['metabolights:Study'], ARRAY['Metabolights'], ARRAY['MTBLS1'], 'MTBLS1', NULL),"
                    + "('ensembl:ENSG1', 'type 2 diabetes mellitus', ARRAY['biolink:Disease','biolink:Gene'], ARRAY['OLS.mondo','HGNC'],"
                    + "   ARRAY['ensembl:ENSG1','mondo:0005148','omim:125853','hgnc:9204'], 'ensembl:ENSG1', 'ENSG1'),"
                    + "('efo:0004318', 'smoking behavior', ARRAY['biolink:PhenotypicFeature'], ARRAY['OLS.efo'], ARRAY['efo:0004318'], 'efo:0004318', 'EFO:0004318'),"
                    + "('efo:0005671', 'smoking behaviour trait', ARRAY['biolink:PhenotypicFeature'], ARRAY['OLS.efo','GWAS'], ARRAY['efo:0005671'], 'efo:0005671', 'EFO:0005671'),"
                    + "('biolink:Human', 'Homo sapiens', ARRAY['biolink:OrganismTaxon'], ARRAY['OLS.ncbitaxon'], ARRAY['biolink:Human','ncbitaxon:9606'], 'biolink:Human', NULL)");
            st.execute("ANALYZE " + TABLE);
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        if (pg == null) {
            return;
        }
        try (Connection conn = pg.getConnection(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + TABLE);
        }
    }

    /** Filters from alternating keys and values; a repeated key gets several values. */
    static Map<String, List<String>> filters(String... keyValue) {
        var filters = new LinkedHashMap<String, List<String>>();
        for (int i = 0; i < keyValue.length; i += 2) {
            filters.computeIfAbsent(keyValue[i], k -> new ArrayList<>()).add(keyValue[i + 1]);
        }
        return filters;
    }

    static List<Object> nodeIds(GrebiPostgresClient.NodeQueryResult result) {
        return result.results.stream().map(r -> r.get("grebi:nodeId")).toList();
    }

    static List<Object> nodeIds(List<Map<String, Object>> rows) {
        return rows.stream().map(r -> r.get("grebi:nodeId")).toList();
    }

    @Test
    void aSourceIdListingCountsWhatItReturns() {
        var found = pg.searchNodes(GRAPH, null, filters("grebi:sourceIds", "GCST1"), false, 0, 10);
        assertEquals(1, found.totalCount);
        assertEquals(List.of("GCST1"), nodeIds(found));
        assertEquals(Map.of("GWAS", 1L), found.facets.get("grebi:datasources"));
        assertEquals(Map.of("gwas:Study", 1L), found.facets.get("grebi:type"));

        var missing = pg.searchNodes(GRAPH, null, filters("grebi:sourceIds", "no-such-id"), false, 0, 10);
        assertEquals(0, missing.totalCount, "the total used to be the planner's guess, 0.5% of the table");
        assertEquals(List.of(), nodeIds(missing));
        assertEquals(Map.of("grebi:type", Map.of(), "grebi:datasources", Map.of()), missing.facets);
    }

    @Test
    void aMergedInIdentifierIsFoundBySourceIdWhileTheCurieIsTheCanonicalOne() {
        assertEquals(List.of("ensembl:ENSG1"), nodeIds(pg.searchNodes(GRAPH, null, filters("grebi:sourceIds", "mondo:0005148"), false, 0, 10)));
        assertEquals(List.of("biolink:Human"), nodeIds(pg.searchNodes(GRAPH, null, filters("grebi:sourceIds", "ncbitaxon:9606"), false, 0, 10)));

        var byMergedCurie = pg.searchNodes(GRAPH, null, filters("grebi:curie", "mondo:0005148"), false, 0, 10);
        assertEquals(0, byMergedCurie.totalCount, "the curie filter matches the canonical CURIE only, and used to count 1 regardless");
        assertEquals(List.of(), nodeIds(byMergedCurie));
        assertEquals(List.of("ensembl:ENSG1"), nodeIds(pg.searchNodes(GRAPH, null, filters("grebi:curie", "ensembl:ENSG1"), false, 0, 10)));
        assertEquals(List.of("efo:0004318"), nodeIds(pg.searchNodes(GRAPH, null, filters("ols:curie", "EFO:0004318"), false, 0, 10)));
        assertEquals(List.of("biolink:Human"), nodeIds(pg.searchNodes(GRAPH, null, filters("grebi:nodeId", "biolink:Human"), false, 0, 10)));
    }

    @Test
    void severalIdentifiersAndTheOtherFiltersCombine() {
        var several = filters("grebi:sourceIds", "GCST1", "grebi:sourceIds", "MTBLS1",
                "grebi:sourceIds", "mondo:0005148", "grebi:sourceIds", "omim:125853", "grebi:sourceIds", "nope");
        var all = pg.searchNodes(GRAPH, null, several, false, 0, 10);
        assertEquals(3, all.totalCount, "the merged node counts once though two of its identifiers were given");
        assertEquals(List.of("MTBLS1", "GCST1", "ensembl:ENSG1"), nodeIds(all), "in name order");
        assertEquals(Map.of("GWAS", 1L, "Metabolights", 1L, "HGNC", 1L, "OLS.mondo", 1L), all.facets.get("grebi:datasources"));

        var narrowed = new LinkedHashMap<>(several);
        narrowed.put("grebi:datasources", List.of("GWAS"));
        var gwasOnly = pg.searchNodes(GRAPH, null, narrowed, false, 0, 10);
        assertEquals(List.of("GCST1"), nodeIds(gwasOnly));
        assertEquals(1, gwasOnly.totalCount);

        var excluded = new LinkedHashMap<>(several);
        excluded.put("-grebi:datasources", List.of("GWAS"));
        assertEquals(List.of("MTBLS1", "ensembl:ENSG1"), nodeIds(pg.searchNodes(GRAPH, null, excluded, false, 0, 10)));

        var named = pg.searchNodes(GRAPH, "study", several, false, 0, 10);
        assertEquals(List.of("MTBLS1"), nodeIds(named), "the text narrows the identifier set");
        assertEquals(1, named.totalCount);

        var contradiction = filters("grebi:nodeId", "GCST1", "grebi:sourceIds", "MTBLS1");
        assertEquals(0, pg.searchNodes(GRAPH, null, contradiction, false, 0, 10).totalCount, "identifier filters AND together");
    }

    @Test
    void theIdentifierPageIsCutFromTheWholeSet() {
        var several = filters("grebi:sourceIds", "GCST1", "grebi:sourceIds", "MTBLS1", "grebi:sourceIds", "mondo:0005148");
        var second = pg.searchNodes(GRAPH, null, several, false, 1, 1);
        assertEquals(3, second.totalCount);
        assertEquals(List.of("GCST1"), nodeIds(second));
        var pastTheEnd = pg.searchNodes(GRAPH, null, several, false, 5, 10);
        assertEquals(3, pastTheEnd.totalCount);
        assertEquals(List.of(), nodeIds(pastTheEnd));
    }

    @Test
    void aNameSearchTotalIsACount() {
        var smoking = pg.searchNodes(GRAPH, "smoking", null, false, 0, 10);
        assertEquals(2, smoking.totalCount);
        assertEquals(2, smoking.results.size());
        assertEquals(Map.of("OLS.efo", 2L, "GWAS", 1L), smoking.facets.get("grebi:datasources"));

        var gwasSmoking = pg.searchNodes(GRAPH, "smoking", filters("grebi:datasources", "GWAS"), false, 0, 10);
        assertEquals(1, gwasSmoking.totalCount);
        assertEquals(List.of("efo:0005671"), nodeIds(gwasSmoking));

        var nothing = pg.searchNodes(GRAPH, "GCST1", filters("grebi:datasources", "GWAS"), false, 0, 10);
        assertEquals(0, nothing.totalCount, "an accession is not a name; the total used to be the planner's guess");
        assertEquals(List.of(), nodeIds(nothing));
        assertEquals(Map.of("grebi:type", Map.of(), "grebi:datasources", Map.of()), nothing.facets);

        var whole = pg.searchNodes(GRAPH, "SMOKING BEHAVIOR", null, true, 0, 10);
        assertEquals(1, whole.totalCount);
        assertEquals(List.of("efo:0004318"), nodeIds(whole));
        assertEquals(0, pg.searchNodes(GRAPH, "smoking_behavior", null, true, 0, 10).totalCount, "a whole-name match takes the text literally");
        assertEquals(0, pg.searchNodes(GRAPH, "smoking%", null, true, 0, 10).totalCount);

        var secondPage = pg.searchNodes(GRAPH, "smoking", null, false, 1, 1);
        assertEquals(2, secondPage.totalCount);
        assertEquals(1, secondPage.results.size());
        var pastTheEnd = pg.searchNodes(GRAPH, "smoking", null, false, 10, 10);
        assertEquals(2, pastTheEnd.totalCount, "an empty page past the end still knows the total");
        assertEquals(List.of(), nodeIds(pastTheEnd));
    }

    @Test
    void aSmallFilteredListingIsCountedToo() {
        var efo = pg.searchNodes(GRAPH, null, filters("grebi:datasources", "OLS.efo"), false, 0, 10);
        assertEquals(2, efo.totalCount);
        assertEquals(List.of("efo:0004318", "efo:0005671"), nodeIds(efo), "in name order");
        assertEquals(Map.of("biolink:PhenotypicFeature", 2L), efo.facets.get("grebi:type"));

        var everything = pg.searchNodes(GRAPH, null, null, false, 0, 10);
        assertEquals(6, everything.results.size());
        assertTrue(everything.facets.isEmpty(), "no facets for the unfiltered listing");
    }

    @Test
    void theLookupFindsNodesBySourceIdNodeIdAndCurieOnceEach() {
        var ids = List.of("GCST1", "mondo:0005148", "omim:125853", "ensembl:ENSG1", "biolink:Human", "EFO:0004318", "nope");
        var rows = pg.lookupNodes(GRAPH, ids, null, 100);
        // The rows come in node id order, deterministic for the cut-off, but
        // which order that is depends on the database's collation.
        assertEquals(Set.of("GCST1", "biolink:Human", "ensembl:ENSG1"), Set.copyOf(nodeIds(rows)),
                "the merged node once; an ols:curie is not an identifier the lookup matches");
        assertEquals(3, rows.size());
        var merged = rows.stream().filter(r -> r.get("grebi:nodeId").equals("ensembl:ENSG1")).findFirst().orElseThrow();
        assertEquals(List.of("ensembl:ENSG1", "mondo:0005148", "omim:125853", "hgnc:9204"), merged.get("grebi:sourceIds"));
        assertEquals("ensembl:ENSG1", merged.get("grebi:curie"));
        assertEquals(List.of("biolink:Disease", "biolink:Gene"), merged.get("grebi:type"));

        assertEquals(nodeIds(rows).subList(0, 2), nodeIds(pg.lookupNodes(GRAPH, ids, null, 2)), "cut at the limit, in the same order");

        var byName = pg.lookupNodes(GRAPH, null, List.of("homo sapiens", "type 2 diabetes mellitus"), 100);
        assertEquals(Set.of("biolink:Human", "ensembl:ENSG1"), Set.copyOf(nodeIds(byName)));
        var both = pg.lookupNodes(GRAPH, List.of("GCST1"), List.of("homo sapiens"), 100);
        assertEquals(Set.of("GCST1", "biolink:Human"), Set.copyOf(nodeIds(both)));
        assertEquals(List.of(), pg.lookupNodes(GRAPH, List.of(), null, 100));
    }
}
