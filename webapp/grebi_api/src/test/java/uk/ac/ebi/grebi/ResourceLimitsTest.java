package uk.ac.ebi.grebi;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ResourceLimitsTest {

    @Test
    void pageRequestsRejectOversizedPagesAndOffsets() {
        var limits = testLimits();

        assertEquals(10, limits.pageRequest(null, null).getPageSize());
        assertThrows(ResourceLimits.ResourceLimitException.class, () -> limits.pageRequest("0", "26"));
        assertThrows(ResourceLimits.ResourceLimitException.class, () -> limits.pageRequest("5", "25"));
    }

    @Test
    void vectorLimitsRejectUnboundedResultCounts() {
        var limits = testLimits();

        assertEquals(10, limits.vectorLimit(null));
        assertThrows(ResourceLimits.ResourceLimitException.class, () -> limits.vectorLimit("0"));
        assertThrows(ResourceLimits.ResourceLimitException.class, () -> limits.vectorLimit("16"));
    }

    @Test
    void textQueryAndBodyLimitsAreEnforced() {
        var limits = testLimits();

        limits.validateText("short", "q");
        assertThrows(ResourceLimits.ResourceLimitException.class, () -> limits.validateText("x".repeat(13), "q"));
        assertThrows(ResourceLimits.ResourceLimitException.class,
                () -> limits.validateRequestBody("x".repeat(21).getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void queryParameterLimitsAreEnforced() {
        var limits = testLimits();

        limits.validateQueryParams(Map.of("a", List.of("b", "c")));
        assertThrows(ResourceLimits.ResourceLimitException.class,
                () -> limits.validateQueryParams(Map.of("a", List.of("b", "c", "d", "e"))));
        assertThrows(ResourceLimits.ResourceLimitException.class,
                () -> limits.validateQueryString("x".repeat(41)));
    }

    @Test
    void rateLimitRejectsRequestsAfterConfiguredWindowBudget() {
        var limits = testLimits();

        limits.checkRateLimit("client");
        limits.checkRateLimit("client");
        var exception = assertThrows(ResourceLimits.ResourceLimitException.class,
                () -> limits.checkRateLimit("client"));
        assertEquals(429, exception.statusCode());
    }

    private static ResourceLimits testLimits() {
        return new ResourceLimits(
                10,
                25,
                100,
                15,
                12,
                40,
                3,
                20,
                2,
                30,
                2,
                60
        );
    }
}
