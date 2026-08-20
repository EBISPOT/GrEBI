package uk.ac.ebi.grebi;

import org.junit.jupiter.api.Test;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Guards the free-text (`q`/`filter`) extraction in the /query and /query.csv
 * handlers. A plain query carries neither param, so the coalesce of the two must
 * return null rather than throw — {@link Objects#requireNonNullElse} threw an NPE
 * here, which 500'd every /query call that had no free-text narrow (the common
 * case, e.g. every query-template integration test).
 */
class QueryParamCoalesceTest {

    @Test
    void firstNonNullReturnsNullWhenAllNull() {
        // The regression: both params absent must yield null, not throw.
        assertNull(GrebiApi.firstNonNull(null, null));
    }

    @Test
    void firstNonNullPrefersEarlierNonNull() {
        assertEquals("q", GrebiApi.firstNonNull("q", "filter"));
        assertEquals("q", GrebiApi.firstNonNull("q", null));
        assertEquals("filter", GrebiApi.firstNonNull(null, "filter"));
    }

    @Test
    void objectsRequireNonNullElseWouldHaveThrown() {
        // Documents why the handlers must not use requireNonNullElse for this.
        assertThrows(NullPointerException.class, () -> Objects.requireNonNullElse(null, null));
    }
}
