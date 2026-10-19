package uk.ac.ebi.grebi;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import uk.ac.ebi.grebi.db.GrebiPostgresClient;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test for the `biolink:broad_match` node search filter: the
 * closure a query template's values_under parameter sends with its
 * autosuggest. Exercises the real GrebiPostgresClient against a live Postgres,
 * like MaterialisedClosureServingTest, and is skipped unless
 * GREBI_TEST_POSTGRES=true (with GREBI_POSTGRES_* pointing at a reachable
 * server that has pg_trgm).
 *
 * To run:
 *   GREBI_TEST_POSTGRES=true GREBI_POSTGRES_HOST=127.0.0.1 GREBI_POSTGRES_PORT=5432 \
 *   GREBI_POSTGRES_USER=grebi GREBI_POSTGRES_DB=grebi \
 *   mvn test -Dtest=NodeSearchClosureTest
 */
class NodeSearchClosureTest {

    static final String GRAPH = "itsearch";
    static final String BROAD_MATCH = GrebiPostgresClient.BROAD_MATCH_FILTER;
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
            st.execute("DROP TABLE IF EXISTS \"nodes_" + GRAPH + "\"");
            st.execute("DROP TABLE IF EXISTS \"edges_" + GRAPH + "\"");
            st.execute("CREATE TABLE \"nodes_" + GRAPH + "\" (\"grebi:nodeId\" TEXT, \"grebi:name\" TEXT,"
                    + " \"grebi:type\" TEXT[] NOT NULL DEFAULT '{}', \"grebi:datasources\" TEXT[] NOT NULL DEFAULT '{}',"
                    + " \"grebi:sourceIds\" TEXT[], \"ols:curie\" TEXT, \"grebi:curie\" TEXT)");
            st.execute("CREATE TABLE \"edges_" + GRAPH + "\" (\"grebi:type\" TEXT, \"grebi:fromNodeId\" TEXT, \"grebi:toNodeId\" TEXT)");

            // The anemias a disease search finds: the merged disease-and-phenotype
            // node and a NCIT grade under the MONDO root; a fern genus, a symptom
            // term and a HP-only phenotype (under the HP root) that are not.
            insertNode(st, "n_disease", "disease", "{mondo:0000001}", "{biolink:Disease}");
            insertNode(st, "n_anemia", "anemia", "{mondo:0002280,hp:0001903}", "{biolink:Disease,biolink:PhenotypicFeature,ols:Class}");
            insertNode(st, "n_anemia_mild", "Anemia (mild)", "{ncit:C82909}", "{biolink:Disease,ols:Class}");
            insertNode(st, "n_fern", "Anemia", "{ncbitaxon:12939}", "{ols:Class}");
            insertNode(st, "n_symp", "anemia", "{symp:0000208}", "{ols:Class}");
            insertNode(st, "n_pheno", "Phenotypic abnormality", "{hp:0000118}", "{biolink:PhenotypicFeature}");
            insertNode(st, "n_fetal", "Fetal anemia", "{hp:0025716}", "{biolink:PhenotypicFeature}");
            // broad_match points descendant -> ancestor (full transitive closure)
            st.execute("INSERT INTO \"edges_" + GRAPH + "\" VALUES "
                    + "('biolink:broad_match','n_anemia','n_disease'),('biolink:broad_match','n_anemia_mild','n_disease'),"
                    + "('biolink:broad_match','n_fetal','n_pheno'),('biolink:related_to','n_fern','n_disease')");
        }
    }

    private static void insertNode(Statement st, String id, String name, String sourceIds, String types) throws Exception {
        st.execute("INSERT INTO \"nodes_" + GRAPH + "\" VALUES ('" + id + "', '" + name + "', '" + types + "', '{IT}', '"
                + sourceIds + "', NULL, '" + id + "')");
    }

    private static Set<String> ids(GrebiPostgresClient.NodeQueryResult res) {
        return res.results.stream().map(r -> (String) r.get("grebi:nodeId")).collect(Collectors.toSet());
    }

    @Test
    void closureKeepsTheRootAndItsDescendantsOnly() {
        assumeTrue(enabled());
        var res = pg.searchNodes(GRAPH, "anemia", Map.of(BROAD_MATCH, List.of("mondo:0000001")), false, 0, 10);
        assertEquals(Set.of("n_anemia", "n_anemia_mild"), ids(res));
        assertEquals(2, res.totalCount, "the count is exact, not the planner's estimate");
        // the whole-name match ranks first
        assertEquals("n_anemia", res.results.get(0).get("grebi:nodeId"));
        assertEquals(Map.of("biolink:Disease", 2L, "biolink:PhenotypicFeature", 1L, "ols:Class", 2L), res.facets.get("grebi:type"));

        // without text: the root itself is in its own closure
        assertEquals(Set.of("n_disease", "n_anemia", "n_anemia_mild"),
                ids(pg.searchNodes(GRAPH, null, Map.of(BROAD_MATCH, List.of("mondo:0000001")), false, 0, 10)));
    }

    @Test
    void repeatedFilterUnitesTheClosures() {
        assumeTrue(enabled());
        var res = pg.searchNodes(GRAPH, "anemia", Map.of(BROAD_MATCH, List.of("mondo:0000001", "hp:0000118")), false, 0, 10);
        assertEquals(Set.of("n_anemia", "n_anemia_mild", "n_fetal"), ids(res));
        assertEquals(3, res.totalCount);
    }

    @Test
    void combinesWithTheOtherFilters() {
        assumeTrue(enabled());
        var res = pg.searchNodes(GRAPH, "anemia",
                Map.of(BROAD_MATCH, List.of("mondo:0000001"), "grebi:type", List.of("biolink:PhenotypicFeature")), false, 0, 10);
        assertEquals(Set.of("n_anemia"), ids(res));
        assertEquals(Set.of("n_anemia_mild"), ids(pg.searchNodes(GRAPH, "anemia",
                Map.of(BROAD_MATCH, List.of("mondo:0000001"), "-grebi:type", List.of("biolink:PhenotypicFeature")), false, 0, 10)));
        assertEquals(Set.of("n_anemia"), ids(pg.searchNodes(GRAPH, "anemia",
                Map.of(BROAD_MATCH, List.of("mondo:0000001")), true, 0, 10)), "exactMatch applies within the closure");
    }

    @Test
    void unknownRootMatchesNothing() {
        assumeTrue(enabled());
        var res = pg.searchNodes(GRAPH, "anemia", Map.of(BROAD_MATCH, List.of("mondo:9999999")), false, 0, 10);
        assertEquals(List.of(), res.results);
        assertEquals(0, res.totalCount);
        assertEquals(Map.of("grebi:type", Map.of(), "grebi:datasources", Map.of()), res.facets, "the facets of an empty search list the facet fields, as any other empty search does");
        // a blank value names nothing either
        assertEquals(0, pg.searchNodes(GRAPH, "anemia", Map.of(BROAD_MATCH, List.of(" ")), false, 0, 10).totalCount);
    }

    @Test
    void withoutTheFilterEveryAnemiaMatches() {
        assumeTrue(enabled());
        var res = pg.searchNodes(GRAPH, "anemia", Map.of(), false, 0, 10);
        assertEquals(Set.of("n_anemia", "n_anemia_mild", "n_fern", "n_symp", "n_fetal"), ids(res));
    }
}
