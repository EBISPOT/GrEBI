package uk.ac.ebi.grebi;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageRequest;
import uk.ac.ebi.grebi.db.GrebiPostgresClient;
import uk.ac.ebi.grebi.repo.GrebiPostgresRepo;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A node's stored blob against a live Postgres: fetched as it is stored, and
 * read abridged to what the whole node gives. The table is the one the
 * dataload makes. Skipped unless GREBI_TEST_POSTGRES=true, as
 * NodeIdentifierQueriesPostgresTest is.
 */
class NodeBlobPostgresTest {

    static final String GRAPH = "itblobs";
    static final String TABLE = "\"blobs_" + GRAPH + "\"";
    static final String NODES = "\"nodes_" + GRAPH + "\"";
    static final byte[] HUB = AbridgementTest.blob(AbridgementTest.translatedHub(30));
    static GrebiPostgresClient pg;

    @BeforeAll
    static void setup() throws Exception {
        assumeTrue(NodeIdentifierQueriesPostgresTest.enabled(), "GREBI_TEST_POSTGRES not set; skipping Postgres integration test");
        pg = new GrebiPostgresClient();
        try (Connection conn = pg.getConnection(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + TABLE);
            st.execute("CREATE TABLE " + TABLE + " (id bytea NOT NULL, json bytea NOT NULL)");
            st.execute("ALTER TABLE " + TABLE + " ALTER COLUMN json SET STORAGE EXTERNAL");
            st.execute("ALTER TABLE " + TABLE + " ADD PRIMARY KEY (id)");
            try (var insert = conn.prepareStatement("INSERT INTO " + TABLE + " VALUES (?, ?)")) {
                insert.setBytes(1, "hub".getBytes(StandardCharsets.UTF_8));
                insert.setBytes(2, HUB);
                insert.execute();
            }
            // the row of the node table the hub is searched by, and a node with no blob
            st.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm");
            st.execute("DROP TABLE IF EXISTS " + NODES);
            st.execute("CREATE TABLE " + NODES + " (\"grebi:nodeId\" TEXT, \"grebi:name\" TEXT, \"grebi:type\" TEXT[],"
                    + " \"grebi:datasources\" TEXT[], \"grebi:sourceIds\" TEXT[], \"grebi:curie\" TEXT, \"ols:curie\" TEXT)");
            st.execute("CREATE INDEX ON " + NODES + " USING gin (\"grebi:sourceIds\")");
            st.execute("INSERT INTO " + NODES + " VALUES "
                    + "('hub', 'APOE', ARRAY['biolink:Gene'], ARRAY['STRING'], ARRAY['hub','hgnc:613'], 'hub', NULL),"
                    + "('blobless', 'a node with no blob', ARRAY['biolink:Gene'], ARRAY['STRING'], ARRAY['blobless'], 'blobless', NULL)");
            st.execute("ANALYZE " + NODES);
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        if (pg == null) {
            return;
        }
        try (Connection conn = pg.getConnection(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + TABLE);
            st.execute("DROP TABLE IF EXISTS " + NODES);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void aSearchResolvesItsHitsWholeOrAbridged() {
        var postgres = new GrebiPostgresRepo();
        var filters = Map.of("grebi:type", List.of("biolink:Gene"));
        var page = PageRequest.of(0, 10);

        var abridged = ResolvedNodes.searchPage(postgres, GRAPH, null, filters, false, true, 5, "fr", true, page);
        assertEquals(2, abridged.getTotalElements());
        var byId = new java.util.HashMap<Object, Map<String, Object>>();
        for (var node : abridged.getContent()) {
            byId.put(node.get("grebi:nodeId"), node);
        }
        var hub = byId.get("hub");
        assertEquals(5, ((List<Object>) hub.get("biolink:interacts_with")).size());
        assertEquals(Map.of("biolink:interacts_with", 30), hub.get(Abridgement.TRUNCATED));
        assertEquals(2, ((List<Object>) hub.get("grebi:name")).size(), "French first, English as the fallback");
        // the node with no blob stays the row the search found
        assertEquals("a node with no blob", byId.get("blobless").get("grebi:name"));

        // the same nodes whole, and as hits
        var whole = ResolvedNodes.searchPage(postgres, GRAPH, null, Map.of("grebi:sourceIds", List.of("hgnc:613")), false, true, null, null, true, page);
        assertEquals(30, ((List<Object>) whole.getContent().get(0).get("biolink:interacts_with")).size());
        var hits = ResolvedNodes.searchPage(postgres, GRAPH, null, Map.of("grebi:sourceIds", List.of("hgnc:613")), false, false, 5, null, true, page);
        assertEquals("APOE", hits.getContent().get(0).get("grebi:name"));
        assertFalse(hits.getContent().get(0).containsKey("biolink:interacts_with"));

        // by id: abridged from the blobs, or whole
        assertEquals(hub.get("biolink:interacts_with"), ResolvedNodes.byId(pg, GRAPH, List.of("hub", "blobless"), 5, "fr", true).get("hub").get("biolink:interacts_with"));
        assertEquals(java.util.Set.of("hub"), ResolvedNodes.byId(pg, GRAPH, List.of("hub", "blobless"), 5, null, false).keySet());
        assertEquals(30, ((List<Object>) ResolvedNodes.byId(pg, GRAPH, List.of("hub"), null, null, true).get("hub").get("biolink:interacts_with")).size());
    }

    @Test
    void aBlobIsFetchedAsItIsStored() {
        assertArrayEquals(HUB, pg.fetchBlob(GRAPH, "hub"));
        assertNull(pg.fetchBlob(GRAPH, "not a node"));
        assertThrows(IllegalArgumentException.class, () -> pg.fetchBlob("no such; graph", "hub"));

        // several at once: the ones there are, by id
        var blobs = pg.fetchBlobs(GRAPH, List.of("not a node", "hub"));
        assertEquals(java.util.Set.of("hub"), blobs.keySet());
        assertArrayEquals(HUB, blobs.get("hub"));
        assertEquals(Map.of(), pg.fetchBlobs(GRAPH, List.of()));
        assertThrows(IllegalArgumentException.class, () -> pg.fetchBlobs("no such; graph", List.of("hub")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void theBlobReadAbridgedIsTheWholeNodeAbridged() {
        var whole = pg.resolveToList(GRAPH, List.of("hub")).get(0);
        for (var lang : new String[] { null, "fr" }) {
            var abridged = Abridgement.read(pg.fetchBlob(GRAPH, "hub"), 5, lang);
            assertEquals(Abridgement.abridge(Languages.localise(whole, lang), 5), abridged);
            assertEquals(5, ((List<Object>) abridged.get("biolink:interacts_with")).size());
            assertEquals(Map.of("biolink:interacts_with", 30), abridged.get(Abridgement.TRUNCATED));
        }
    }
}
