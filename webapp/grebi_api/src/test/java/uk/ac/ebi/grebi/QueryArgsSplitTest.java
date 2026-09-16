package uk.ac.ebi.grebi;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonParser;

import uk.ac.ebi.grebi.db.MaterialisedBuild;
import uk.ac.ebi.grebi.repo.QueryTemplate;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * How a /query request's query string splits into template parameters and
 * facet selections (result-column keys, honoured on the materialised path).
 */
class QueryArgsSplitTest {

    private static QueryTemplate template(String... paramIds) {
        var t = new QueryTemplate();
        t.params = new java.util.ArrayList<>();
        for (String id : paramIds) {
            var p = new QueryTemplate.Parameter();
            p.param_id = id;
            t.params.add(p);
        }
        return t;
    }

    private static MaterialisedBuild build() {
        return MaterialisedBuild.fromMetadata(JsonParser.parseString(
                "{\"id\":\"q\",\"table\":\"matq_q\",\"columns\":["
                + "{\"column_id\":\"cell\",\"column_type\":\"GraphNodeId\"},"
                + "{\"column_id\":\"trait\",\"column_type\":\"string\",\"facet\":true}]}"));
    }

    @Test
    void resultColumnKeysAreSelectionsOnAMaterialisedBuild() {
        var args = GrebiApi.splitQueryArgs(template("cell_id"), build(), Map.of(
                "cell_id", List.of("ex:A"),
                "trait", List.of("bravo", "delta"),
                "page", List.of("0"),
                "q", List.of("x"),
                "bogus", List.of("1")));
        assertEquals(Map.of("cell_id", List.of("ex:A"), "bogus", List.of("1")), args.params);
        assertEquals(Map.of("trait", List.of("bravo", "delta")), args.filters);
    }

    @Test
    void withoutABuildEveryKeyIsAParameter() {
        var args = GrebiApi.splitQueryArgs(template("cell_id"), null, Map.of(
                "cell_id", List.of("ex:A"), "trait", List.of("bravo")));
        assertEquals(Map.of("cell_id", List.of("ex:A"), "trait", List.of("bravo")), args.params);
        assertTrue(args.filters.isEmpty());
    }

    @Test
    void aTemplateParameterWinsOverAColumnOfTheSameName() {
        var args = GrebiApi.splitQueryArgs(template("cell_id", "trait"), build(), Map.of("trait", List.of("bravo")));
        assertEquals(Map.of("trait", List.of("bravo")), args.params);
        assertTrue(args.filters.isEmpty());
    }
}
