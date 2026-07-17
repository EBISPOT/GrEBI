package uk.ac.ebi.grebi.repo;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for the shared result-value normalization applied by BOTH the live
 * Cypher path and the Postgres-materialised path, so the two backends return
 * identical JSON types for scalar columns (e.g. a float column whose value is
 * stored as a JSON string still serialises as a number).
 */
class NormalizeResultValueTest {

    private static QueryTemplate.ResultColumn col(String type) {
        var c = new QueryTemplate.ResultColumn();
        c.column_id = "x";
        c.column_type = type;
        return c;
    }

    @Test
    void floatStringBecomesNumber() {
        // GWAS stores or_or_beta as a JSON string; a float column must serialise as a number.
        assertEquals(Double.valueOf(1.23), GrebiCypherRepo.normalizeResultValue(col("float"), "1.23"));
        assertEquals(Double.valueOf(1.23), GrebiCypherRepo.normalizeResultValue(col("float"), 1.23));
        assertNull(GrebiCypherRepo.normalizeResultValue(col("float"), ""));
        assertNull(GrebiCypherRepo.normalizeResultValue(col("float"), null));
    }

    @Test
    void intStringBecomesInteger() {
        assertEquals(Integer.valueOf(5), GrebiCypherRepo.normalizeResultValue(col("int"), "5"));
        assertEquals(Integer.valueOf(5), GrebiCypherRepo.normalizeResultValue(col("integer"), 5.0));
    }

    @Test
    void datasourceListNormalised() {
        assertEquals(List.of("a"), GrebiCypherRepo.normalizeResultValue(col("DatasourceList"), "a"));
        assertEquals(List.of("a", "b"), GrebiCypherRepo.normalizeResultValue(col("DatasourceList"), List.of("a", "b")));
    }

    @Test
    void stringPassthrough() {
        assertEquals("hi", GrebiCypherRepo.normalizeResultValue(col("string"), "hi"));
    }
}
