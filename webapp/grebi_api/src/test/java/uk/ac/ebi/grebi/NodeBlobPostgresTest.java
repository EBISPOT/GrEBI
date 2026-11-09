package uk.ac.ebi.grebi;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import uk.ac.ebi.grebi.db.GrebiPostgresClient;

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

    @Test
    void aBlobIsFetchedAsItIsStored() {
        assertArrayEquals(HUB, pg.fetchBlob(GRAPH, "hub"));
        assertNull(pg.fetchBlob(GRAPH, "not a node"));
        assertThrows(IllegalArgumentException.class, () -> pg.fetchBlob("no such; graph", "hub"));
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
