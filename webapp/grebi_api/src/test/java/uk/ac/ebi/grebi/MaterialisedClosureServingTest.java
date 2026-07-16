package uk.ac.ebi.grebi;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import uk.ac.ebi.grebi.db.GrebiPostgresClient;
import uk.ac.ebi.grebi.db.GrebiPostgresClient.ClosureParam;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test for closure-at-query-time serving of materialised
 * parameterised templates. Exercises the real GrebiPostgresClient against a live
 * Postgres. Skipped unless GREBI_TEST_POSTGRES=true (and GREBI_POSTGRES_* point at
 * a reachable server), so it never runs in the default unit-test build.
 *
 * To run:
 *   GREBI_TEST_POSTGRES=true GREBI_POSTGRES_HOST=127.0.0.1 GREBI_POSTGRES_PORT=54329 \
 *   GREBI_POSTGRES_USER=grebi GREBI_POSTGRES_DB=grebi \
 *   mvn test -Dtest=MaterialisedClosureServingTest
 */
class MaterialisedClosureServingTest {

    static final String GRAPH = "itclosure";
    static GrebiPostgresClient pg;

    static boolean enabled() {
        return "true".equalsIgnoreCase(System.getenv("GREBI_TEST_POSTGRES"));
    }

    @BeforeAll
    static void setup() throws Exception {
        assumeTrue(enabled(), "GREBI_TEST_POSTGRES not set; skipping Postgres integration test");
        pg = new GrebiPostgresClient();
        try (Connection conn = pg.getConnection(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS \"nodes_" + GRAPH + "\"");
            st.execute("DROP TABLE IF EXISTS \"edges_" + GRAPH + "\"");
            st.execute("DROP TABLE IF EXISTS \"materialised_queries_" + GRAPH + "\"");
            st.execute("CREATE TABLE \"nodes_" + GRAPH + "\" (\"grebi:nodeId\" TEXT, \"grebi:sourceIds\" TEXT[])");
            st.execute("CREATE TABLE \"edges_" + GRAPH + "\" (\"grebi:type\" TEXT, \"grebi:fromNodeId\" TEXT, \"grebi:toNodeId\" TEXT)");
            st.execute("CREATE TABLE \"materialised_queries_" + GRAPH + "\" (query_id TEXT, row_number INT, data JSONB)");

            // A <- B <- C <- D hierarchy (nodeId distinct from curie) + isolated X
            st.execute("INSERT INTO \"nodes_" + GRAPH + "\" VALUES " +
                    "('grp_A', ARRAY['ex:A','EX:A']),('grp_B', ARRAY['ex:B'])," +
                    "('grp_C', ARRAY['ex:C']),('grp_D', ARRAY['ex:D']),('grp_X', ARRAY['ex:X'])");
            // broad_match points descendant -> ancestor (full transitive closure)
            st.execute("INSERT INTO \"edges_" + GRAPH + "\" VALUES " +
                    "('biolink:broad_match','grp_B','grp_A'),('biolink:broad_match','grp_C','grp_A')," +
                    "('biolink:broad_match','grp_C','grp_B'),('biolink:broad_match','grp_D','grp_A')," +
                    "('biolink:broad_match','grp_D','grp_B'),('biolink:broad_match','grp_D','grp_C')");
            // score is a "float" column stored as a JSON *string*; C's is non-numeric
            // ("NR") to exercise the tolerant numeric sort.
            st.execute("INSERT INTO \"materialised_queries_" + GRAPH + "\" VALUES " +
                    "('q1',1,'{\"cell\":{\"id\":[\"ex:A\"],\"grebi:nodeId\":\"" + GRAPH + ":grp_A\",\"grebi:name\":[\"A\"]},\"trait\":\"alpha\",\"score\":\"3.0\"}')," +
                    "('q1',2,'{\"cell\":{\"id\":[\"ex:B\"],\"grebi:nodeId\":\"" + GRAPH + ":grp_B\",\"grebi:name\":[\"B\"]},\"trait\":\"bravo\",\"score\":\"1.0\"}')," +
                    "('q1',3,'{\"cell\":{\"id\":[\"ex:C\"],\"grebi:nodeId\":\"" + GRAPH + ":grp_C\",\"grebi:name\":[\"C\"]},\"trait\":\"charlie\",\"score\":\"NR\"}')," +
                    "('q1',4,'{\"cell\":{\"id\":[\"ex:D\"],\"grebi:nodeId\":\"" + GRAPH + ":grp_D\",\"grebi:name\":[\"D\"]},\"trait\":\"delta\",\"score\":\"2.0\"}')," +
                    "('q1',5,'{\"cell\":{\"id\":[\"ex:X\"],\"grebi:nodeId\":\"" + GRAPH + ":grp_X\",\"grebi:name\":[\"X\"]},\"trait\":\"xray\",\"score\":\"9.0\"}')");
            st.execute("INSERT INTO \"materialised_queries_" + GRAPH + "\" VALUES " +
                    "('q2',1,'{\"cell\":{\"id\":[\"ex:A\"]},\"_count\":10}')," +
                    "('q2',2,'{\"cell\":{\"id\":[\"ex:B\"]},\"_count\":20}')," +
                    "('q2',3,'{\"cell\":{\"id\":[\"ex:C\"]},\"_count\":30}')," +
                    "('q2',4,'{\"cell\":{\"id\":[\"ex:D\"]},\"_count\":40}')");
        }
    }

    private long count(String closure, String curie) {
        var res = pg.searchMaterialisedParameterised(GRAPH, "q1",
                List.of(new ClosureParam("cell", closure, curie)),
                null, Map.of(), List.of(), null, true, false, 0, 100);
        return res.totalCount;
    }

    @Test
    void descendantsClosure() {
        assumeTrue(enabled());
        assertEquals(4, count("descendants", "ex:A"), "A + descendants B,C,D");
        assertEquals(3, count("descendants", "ex:B"), "B + descendants C,D");
        assertEquals(2, count("descendants", "ex:C"));
        assertEquals(1, count("descendants", "ex:D"));
        assertEquals(1, count("descendants", "ex:X"), "isolated node -> just itself");
        assertEquals(0, count("descendants", "ex:UNKNOWN"), "unknown node -> no rows");
    }

    @Test
    void exactClosure() {
        assumeTrue(enabled());
        assertEquals(1, count("exact", "ex:B"));
        // exact via a clique-member curie still resolves through the node
        assertEquals(1, count("exact", "EX:A"));
    }

    @Test
    void ancestorsClosure() {
        assumeTrue(enabled());
        assertEquals(4, count("ancestors", "ex:D"), "D + ancestors A,B,C");
        assertEquals(2, count("ancestors", "ex:B"), "B + ancestor A");
    }

    @Test
    void resultRowsProjectedAndPrefixStripped() {
        assumeTrue(enabled());
        var res = pg.searchMaterialisedParameterised(GRAPH, "q1",
                List.of(new ClosureParam("cell", "exact", "ex:A")),
                null, Map.of(), List.of(), null, true, false, 0, 100);
        assertEquals(1, res.results.size());
        @SuppressWarnings("unchecked")
        var cell = (Map<String, Object>) res.results.get(0).get("cell");
        // graph: prefix stripped from the stored nodeId
        assertEquals("grp_A", cell.get("grebi:nodeId"));
    }

    @Test
    void countsOnlySumsOverClosure() {
        assumeTrue(enabled());
        assertEquals(100, pg.sumMaterialisedParameterisedCounts(GRAPH, "q2",
                List.of(new ClosureParam("cell", "descendants", "ex:A"))));
        assertEquals(90, pg.sumMaterialisedParameterisedCounts(GRAPH, "q2",
                List.of(new ClosureParam("cell", "descendants", "ex:B"))));
        assertEquals(30, pg.sumMaterialisedParameterisedCounts(GRAPH, "q2",
                List.of(new ClosureParam("cell", "exact", "ex:C"))));
    }

    @Test
    void searchTextFilters() {
        assumeTrue(enabled());
        var res = pg.searchMaterialisedParameterised(GRAPH, "q1",
                List.of(new ClosureParam("cell", "descendants", "ex:A")),
                "charlie", Map.of(), List.of(), null, true, false, 0, 100);
        assertEquals(1, res.totalCount, "free-text 'charlie' matches only the C row");
    }

    @Test
    void numericSortToleratesNonNumericAndIsStable() {
        assumeTrue(enabled());
        // sort descendants(A) = {A,B,C,D} by the "score" float column ascending.
        // Numeric scores 3.0/1.0/2.0 sort B,D,A; C's "NR" is non-numeric -> NULLS LAST.
        var res = pg.searchMaterialisedParameterised(GRAPH, "q1",
                List.of(new ClosureParam("cell", "descendants", "ex:A")),
                null, Map.of(), List.of(), "score", true, true, 0, 100);
        assertEquals(4, res.totalCount);
        var order = res.results.stream()
                .map(r -> ((java.util.List<?>) ((Map<?, ?>) r.get("cell")).get("grebi:name")).get(0))
                .toList();
        assertEquals(List.of("B", "D", "A", "C"), order,
                "numeric asc with the non-numeric 'NR' sorted last (no exception)");
    }

    @Test
    void streamCollectsClosureRows() {
        assumeTrue(enabled());
        var collected = new java.util.ArrayList<Map<String, Object>>();
        pg.streamMaterialisedParameterised(GRAPH, "q1",
                List.of(new ClosureParam("cell", "descendants", "ex:B")),
                null, true, false, collected::add);
        assertEquals(3, collected.size(), "stream yields descendants(B) = B,C,D");
    }
}
