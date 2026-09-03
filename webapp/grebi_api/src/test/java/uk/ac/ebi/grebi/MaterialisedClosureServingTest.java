package uk.ac.ebi.grebi;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonParser;

import uk.ac.ebi.grebi.db.GrebiPostgresClient;
import uk.ac.ebi.grebi.db.GrebiPostgresClient.ClosureParam;
import uk.ac.ebi.grebi.db.MaterialisedBuild;

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
    static final String Q1_TABLE = "matq_" + GRAPH + "_q1";
    static final String Q2_TABLE = "matq_" + GRAPH + "_q2";
    static final String Q3_TABLE = "matq_" + GRAPH + "_q3";
    static GrebiPostgresClient pg;
    static MaterialisedBuild q1;   // older build: curie arrays, && overlap
    static MaterialisedBuild q2;   // counts_only histogram (curie arrays)
    static MaterialisedBuild q3;   // current build: node ids, closure_key=nid

    static boolean enabled() {
        return "true".equalsIgnoreCase(System.getenv("GREBI_TEST_POSTGRES"));
    }

    static MaterialisedBuild buildFromJson(String json) {
        return MaterialisedBuild.fromMetadata(JsonParser.parseString(json));
    }

    @BeforeAll
    static void setup() throws Exception {
        assumeTrue(enabled(), "GREBI_TEST_POSTGRES not set; skipping Postgres integration test");
        pg = new GrebiPostgresClient();

        // The build descriptors serving would normally read from graph_metadata.
        q1 = buildFromJson("{\"id\":\"q1\",\"table\":\"" + Q1_TABLE + "\",\"mode\":\"full\",\"columns\":["
                + "{\"column_id\":\"cell\",\"column_type\":\"GraphNodeId\",\"facet\":true},"
                + "{\"column_id\":\"trait\",\"column_type\":\"string\",\"facet\":true},"
                + "{\"column_id\":\"score\",\"column_type\":\"float\"},"
                + "{\"column_id\":\"ds\",\"column_type\":\"DatasourceList\",\"facet\":true}],"
                + "\"params\":[{\"param_id\":\"cell_id\",\"filters_column\":\"cell\",\"closure\":\"descendants\",\"param_type\":\"SourceId\"}]}");
        q2 = buildFromJson("{\"id\":\"q2\",\"table\":\"" + Q2_TABLE + "\",\"mode\":\"counts_only\",\"columns\":["
                + "{\"column_id\":\"cell\",\"column_type\":\"GraphNodeId\"},"
                + "{\"column_id\":\"_count\",\"column_type\":\"int\"}],"
                + "\"params\":[{\"param_id\":\"cell_id\",\"filters_column\":\"cell\",\"closure\":\"descendants\",\"param_type\":\"SourceId\"}]}");
        q3 = buildFromJson("{\"id\":\"q3\",\"table\":\"" + Q3_TABLE + "\",\"mode\":\"full\",\"closure_key\":\"nid\",\"columns\":["
                + "{\"column_id\":\"cell\",\"column_type\":\"GraphNodeId\",\"facet\":true},"
                + "{\"column_id\":\"trait\",\"column_type\":\"string\",\"facet\":true}],"
                + "\"params\":[{\"param_id\":\"cell_id\",\"filters_column\":\"cell\",\"closure\":\"descendants\",\"param_type\":\"SourceId\"}]}");

        try (Connection conn = pg.getConnection(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS \"nodes_" + GRAPH + "\"");
            st.execute("DROP TABLE IF EXISTS \"edges_" + GRAPH + "\"");
            st.execute("DROP TABLE IF EXISTS \"" + Q1_TABLE + "\"");
            st.execute("DROP TABLE IF EXISTS \"" + Q2_TABLE + "\"");
            st.execute("DROP TABLE IF EXISTS \"" + Q3_TABLE + "\"");
            st.execute("CREATE TABLE \"nodes_" + GRAPH + "\" (\"grebi:nodeId\" TEXT, \"grebi:sourceIds\" TEXT[])");
            st.execute("CREATE TABLE \"edges_" + GRAPH + "\" (\"grebi:type\" TEXT, \"grebi:fromNodeId\" TEXT, \"grebi:toNodeId\" TEXT)");
            // The typed per-query tables the pgcopy writer would produce.
            st.execute("CREATE TABLE \"" + Q1_TABLE + "\" (row_number INT NOT NULL,"
                    + " cell_id TEXT[] NOT NULL DEFAULT '{}', cell_name TEXT,"
                    + " trait TEXT, score double precision, ds TEXT[] NOT NULL DEFAULT '{}',"
                    + " payload BYTEA NOT NULL)");
            st.execute("CREATE TABLE \"" + Q2_TABLE + "\" (row_number INT NOT NULL,"
                    + " cell_id TEXT[] NOT NULL DEFAULT '{}', cell_name TEXT,"
                    + " \"_count\" bigint, payload BYTEA NOT NULL)");

            // A <- B <- C <- D hierarchy (nodeId distinct from curie) + isolated X
            st.execute("INSERT INTO \"nodes_" + GRAPH + "\" VALUES " +
                    "('grp_A', ARRAY['ex:A','EX:A']),('grp_B', ARRAY['ex:B'])," +
                    "('grp_C', ARRAY['ex:C']),('grp_D', ARRAY['ex:D']),('grp_X', ARRAY['ex:X'])");
            // broad_match points descendant -> ancestor (full transitive closure)
            st.execute("INSERT INTO \"edges_" + GRAPH + "\" VALUES " +
                    "('biolink:broad_match','grp_B','grp_A'),('biolink:broad_match','grp_C','grp_A')," +
                    "('biolink:broad_match','grp_C','grp_B'),('biolink:broad_match','grp_D','grp_A')," +
                    "('biolink:broad_match','grp_D','grp_B'),('biolink:broad_match','grp_D','grp_C')");
            // C's score was the non-numeric "NR" upstream: the typed writer stores
            // NULL, which the numeric sort puts last (like the live toFloat()).
            insertQ1Row(st, 1, "A", "ex:A", "grp_A", "alpha", "3.0", "ARRAY['X','Y']");
            insertQ1Row(st, 2, "B", "ex:B", "grp_B", "bravo", "1.0", "ARRAY['X']");
            insertQ1Row(st, 3, "C", "ex:C", "grp_C", "charlie", "NULL", "ARRAY['Y']");
            insertQ1Row(st, 4, "D", "ex:D", "grp_D", "delta", "2.0", "ARRAY['X']");
            insertQ1Row(st, 5, "X", "ex:X", "grp_X", "xray", "9.0", "ARRAY['Z']");
            st.execute("INSERT INTO \"" + Q2_TABLE + "\" VALUES " +
                    "(1, ARRAY['ex:A'], 'A', 10, convert_to('{}','UTF8'))," +
                    "(2, ARRAY['ex:B'], 'B', 20, convert_to('{}','UTF8'))," +
                    "(3, ARRAY['ex:C'], 'C', 30, convert_to('{}','UTF8'))," +
                    "(4, ARRAY['ex:D'], 'D', 40, convert_to('{}','UTF8'))");

            // Current writer layout: the base node's bare id in cell_nid (no
            // curie array), matched by node id against the closure.
            st.execute("CREATE TABLE \"" + Q3_TABLE + "\" (row_number INT NOT NULL,"
                    + " cell_nid TEXT, cell_name TEXT, trait TEXT, payload BYTEA NOT NULL)");
            st.execute("CREATE INDEX ON \"" + Q3_TABLE + "\" (cell_nid)");
            for (String[] r : new String[][]{{"1","grp_A","A","alpha"},{"2","grp_B","B","bravo"},
                    {"3","grp_C","C","charlie"},{"4","grp_D","D","delta"},{"5","grp_X","X","xray"}}) {
                String payload = "{\"cell\":{\"grebi:nodeId\":\"" + GRAPH + ":" + r[1]
                        + "\",\"grebi:name\":[\"" + r[2] + "\"]},\"trait\":\"" + r[3] + "\"}";
                st.execute("INSERT INTO \"" + Q3_TABLE + "\" VALUES (" + r[0] + ", '" + r[1] + "', '"
                        + r[2] + "', '" + r[3] + "', convert_to('" + payload + "','UTF8'))");
            }
        }
    }

    private long countNid(String closure, String curie) {
        var res = pg.searchMaterialisedParameterised(GRAPH, q3,
                List.of(new ClosureParam("cell", closure, curie)),
                null, List.of(), null, true, 0, 100);
        return res.totalCount;
    }

    @Test
    void nodeIdClosureMatchesCurieClosure() {
        assumeTrue(enabled());
        // the nid build must answer exactly like the curie-array build
        for (String curie : List.of("ex:A", "ex:B", "ex:C", "ex:D", "ex:X", "ex:UNKNOWN")) {
            assertEquals(count("descendants", curie), countNid("descendants", curie), "descendants " + curie);
            assertEquals(count("exact", curie), countNid("exact", curie), "exact " + curie);
        }
        assertEquals(1, countNid("exact", "EX:A"), "exact via a clique-member curie");
        assertEquals(4, countNid("ancestors", "ex:D"));
    }

    @Test
    void nodeIdBuildServesRowsAndFacets() {
        assumeTrue(enabled());
        var res = pg.searchMaterialisedParameterised(GRAPH, q3,
                List.of(new ClosureParam("cell", "descendants", "ex:B")),
                null,
                List.of(new GrebiPostgresClient.FacetField("cell", GrebiPostgresClient.FacetKind.NODE_NAME)),
                "trait", false, 0, 100);
        assertEquals(3, res.totalCount);
        // sorted by trait desc: delta, charlie, bravo — payload nodeIds prefix-stripped
        var order = res.results.stream()
                .map(r -> ((Map<?, ?>) r.get("cell")).get("grebi:nodeId")).toList();
        assertEquals(List.of("grp_D", "grp_C", "grp_B"), order);
        assertEquals(Map.of("B", 1L, "C", 1L, "D", 1L), res.facets.get("cell"));
    }

    private static void insertQ1Row(Statement st, int rowNum, String name, String curie,
            String grp, String trait, String score, String dsArray) throws Exception {
        String payload = "{\"cell\":{\"id\":[\"" + curie + "\"],\"grebi:nodeId\":\"" + GRAPH + ":" + grp
                + "\",\"grebi:name\":[\"" + name + "\"]},\"trait\":\"" + trait + "\"}";
        st.execute("INSERT INTO \"" + Q1_TABLE + "\" VALUES (" + rowNum + ", ARRAY['" + curie + "'], '"
                + name + "', '" + trait + "', " + score + ", " + dsArray
                + ", convert_to('" + payload + "','UTF8'))");
    }

    private long count(String closure, String curie) {
        var res = pg.searchMaterialisedParameterised(GRAPH, q1,
                List.of(new ClosureParam("cell", closure, curie)),
                null, List.of(), null, true, 0, 100);
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
        var res = pg.searchMaterialisedParameterised(GRAPH, q1,
                List.of(new ClosureParam("cell", "exact", "ex:A")),
                null, List.of(), null, true, 0, 100);
        assertEquals(1, res.results.size());
        @SuppressWarnings("unchecked")
        var cell = (Map<String, Object>) res.results.get(0).get("cell");
        // graph: prefix stripped from the stored (payload) nodeId
        assertEquals("grp_A", cell.get("grebi:nodeId"));
    }

    @Test
    void countsOnlySumsOverClosure() {
        assumeTrue(enabled());
        assertEquals(100, pg.sumMaterialisedParameterisedCounts(GRAPH, q2,
                List.of(new ClosureParam("cell", "descendants", "ex:A"))));
        assertEquals(90, pg.sumMaterialisedParameterisedCounts(GRAPH, q2,
                List.of(new ClosureParam("cell", "descendants", "ex:B"))));
        assertEquals(30, pg.sumMaterialisedParameterisedCounts(GRAPH, q2,
                List.of(new ClosureParam("cell", "exact", "ex:C"))));
    }

    @Test
    void numericSortToleratesNullAndIsStable() {
        assumeTrue(enabled());
        // sort descendants(A) = {A,B,C,D} by the typed "score" column ascending.
        // Numeric scores 3.0/1.0/2.0 sort B,D,A; C's score is NULL -> NULLS LAST.
        var res = pg.searchMaterialisedParameterised(GRAPH, q1,
                List.of(new ClosureParam("cell", "descendants", "ex:A")),
                null, List.of(), "score", true, 0, 100);
        assertEquals(4, res.totalCount);
        var order = res.results.stream()
                .map(r -> ((java.util.List<?>) ((Map<?, ?>) r.get("cell")).get("grebi:name")).get(0))
                .toList();
        assertEquals(List.of("B", "D", "A", "C"), order,
                "numeric asc with the NULL score sorted last (no exception)");
    }

    @Test
    void streamCollectsClosureRows() {
        assumeTrue(enabled());
        var collected = new java.util.ArrayList<Map<String, Object>>();
        pg.streamMaterialisedParameterised(GRAPH, q1,
                List.of(new ClosureParam("cell", "descendants", "ex:B")), null,
                null, true, collected::add);
        assertEquals(3, collected.size(), "stream yields descendants(B) = B,C,D");
    }

    @Test
    void freeTextNarrowsRows() {
        assumeTrue(enabled());
        var res = pg.searchMaterialisedParameterised(GRAPH, q1,
                List.of(new ClosureParam("cell", "descendants", "ex:A")),
                "charlie", List.of(), null, true, 0, 100);
        assertEquals(1, res.totalCount, "free-text 'charlie' matches only the C row");
    }

    @Test
    void facetsBreakDownColumns() {
        assumeTrue(enabled());
        // descendants(A) = {A,B,C,D}
        var res = pg.searchMaterialisedParameterised(GRAPH, q1,
                List.of(new ClosureParam("cell", "descendants", "ex:A")),
                null,
                List.of(
                        new GrebiPostgresClient.FacetField("cell", GrebiPostgresClient.FacetKind.NODE_NAME),
                        new GrebiPostgresClient.FacetField("trait", GrebiPostgresClient.FacetKind.SCALAR),
                        new GrebiPostgresClient.FacetField("ds", GrebiPostgresClient.FacetKind.ARRAY)),
                null, true, 0, 100);

        // node-name facet: one row per cell in the closure
        assertEquals(Map.of("A", 1L, "B", 1L, "C", 1L, "D", 1L), res.facets.get("cell"));
        // scalar facet
        assertEquals(Map.of("alpha", 1L, "bravo", 1L, "charlie", 1L, "delta", 1L), res.facets.get("trait"));
        // array facet: X in A,B,D = 3; Y in A,C = 2 (Z's row is X-node, outside the closure)
        assertEquals(Map.of("X", 3L, "Y", 2L), res.facets.get("ds"));
    }

    @Test
    void facetsRespectFreeText() {
        assumeTrue(enabled());
        var res = pg.searchMaterialisedParameterised(GRAPH, q1,
                List.of(new ClosureParam("cell", "descendants", "ex:A")),
                "charlie",
                List.of(new GrebiPostgresClient.FacetField("trait", GrebiPostgresClient.FacetKind.SCALAR)),
                null, true, 0, 100);
        // the free-text narrow applies to the facet too
        assertEquals(Map.of("charlie", 1L), res.facets.get("trait"));
    }
}
