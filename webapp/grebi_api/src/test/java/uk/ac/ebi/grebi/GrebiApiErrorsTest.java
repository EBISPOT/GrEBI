package uk.ac.ebi.grebi;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/** Cross-cutting behaviour: limits, error bodies, CORS. */
class GrebiApiErrorsTest {

    TestApp app;

    @BeforeEach
    void start() {
        app = TestApp.start();
    }

    @AfterEach
    void stop() {
        app.close();
    }

    @Test
    void limitViolationsAre400sWithAJsonBody() {
        var tooBig = app.get("/api/v1/graphs/g1/nodes?size=1000");
        assertEquals(400, tooBig.status());
        assertEquals("application/json", tooBig.header("content-type"));
        assertTrue(tooBig.json().getAsJsonObject().get("error").getAsString().contains(String.valueOf(ResourceLimits.DEFAULT_MAX_PAGE_SIZE)));

        var longQuery = app.get("/api/v1/graphs/g1/search?q=" + "x".repeat(ResourceLimits.DEFAULT_MAX_TEXT_CHARS + 1));
        assertEquals(400, longQuery.status());
        // every query parameter value is checked before the route sees it
        assertEquals("Query parameter value may not exceed " + ResourceLimits.DEFAULT_MAX_TEXT_CHARS + " characters",
            longQuery.json().getAsJsonObject().get("error").getAsString());
    }

    @Test
    void repositoryFailuresAre500sWithAJsonBody() {
        when(app.postgres.autocomplete(any(), any())).thenThrow(new RuntimeException("boom"));
        var res = app.get("/api/v1/graphs/g1/suggest?q=x");
        assertEquals(500, res.status());
        assertEquals("application/json", res.header("content-type"));
        assertEquals("boom", res.json().getAsJsonObject().get("error").getAsString());
    }

    @Test
    void anExceptionWithoutAMessageStillGetsAJsonErrorBody() {
        // the metadata repository throws a bare NullPointerException for an unknown graph
        when(app.metadata.getMetadata("nope")).thenThrow(new NullPointerException());
        var res = app.get("/api/v1/graphs/nope");
        assertEquals(500, res.status());
        assertEquals("application/json", res.header("content-type"));
        assertEquals("NullPointerException", res.json().getAsJsonObject().get("error").getAsString());
        // the app is still serving afterwards
        assertEquals(200, app.get("/api/health").status());
    }

    @Test
    void unknownRoutesAre404() {
        assertEquals(404, app.get("/api/v1/nope").status());
    }

    @Test
    void anyOriginIsAllowed() {
        var res = app.get("/api/health", Map.of("Origin", "https://example.org"));
        assertEquals(200, res.status());
        assertNotNull(res.header("access-control-allow-origin"), "CORS header");
    }
}
