package uk.ac.ebi.grebi;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import uk.ac.ebi.grebi.db.GrebiPostgresClient;
import uk.ac.ebi.grebi.repo.GrebiPostgresRepo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** The bulk identifier lookup: parsing, matching and the answer's shape, without a database. */
class NodeLookupTest {

    static final String IRI = "http://purl.obolibrary.org/obo/MONDO_0005083";

    final GrebiPostgresRepo postgres = mock(GrebiPostgresRepo.class);
    final GrebiPostgresClient pgClient = mock(GrebiPostgresClient.class);
    /** at most 3 identifiers of at most 64 characters */
    final ResourceLimits limits = new ResourceLimits(10, 25, 100, 15, 64, 64, 30, 4096, 2, 3, 30, 0, 60);
    /** the prefix service, as far as these tests need one: only the MONDO IRI has a prefix */
    final NodeLookup lookup = new NodeLookup(postgres, limits,
        ids -> ids.stream().map(id -> id.equals(IRI) ? "mondo:0005083" : id).toList());

    static Map<String, Object> row(String nodeId, String name, List<String> sourceIds) {
        return TestApp.row("grebi:nodeId", nodeId, "grebi:name", name, "grebi:type", List.of("biolink:Disease"),
            "grebi:datasources", List.of("OLS.mondo"), "grebi:sourceIds", sourceIds, "grebi:curie", nodeId);
    }

    @Test
    void theBodyIsAnObjectWithIdsAnArrayOrOneIdentifierPerLine() {
        assertEquals(List.of("mondo:1", "hgnc:2"), NodeLookup.parseIds("{\"ids\": [\" mondo:1 \", \"hgnc:2\", \"\", \"mondo:1\"]}"),
            "trimmed, blanks and repeats dropped, order kept");
        assertEquals(List.of("a", "7"), NodeLookup.parseIds("[\"a\", 7]"));
        assertEquals(List.of("a", "b", "c"), NodeLookup.parseIds("a\r\nb\n\n c \n"));
        assertEquals(List.of(), NodeLookup.parseIds(""));
        assertEquals(List.of(), NodeLookup.parseIds(null));
    }

    @Test
    void malformedBodiesAreBadRequests() {
        for (var body : List.of("{\"nope\": 1}", "{\"ids\": \"a\"}", "{not json", "[{\"x\": 1}]", "[\"a\"")) {
            var rejected = assertThrows(ResourceLimits.ResourceLimitException.class, () -> NodeLookup.parseIds(body), body);
            assertEquals(400, rejected.statusCode());
        }
    }

    @Test
    void anIdentifierIsLookedForAsGivenLowerCasedAndNormalised() {
        assertEquals(List.of("MONDO:0005083", "mondo:0005083"), List.copyOf(NodeLookup.candidates("MONDO:0005083", "MONDO:0005083")));
        assertEquals(List.of(IRI, IRI.toLowerCase(), "mondo:0005083"), List.copyOf(NodeLookup.candidates(IRI, "mondo:0005083")));
        assertEquals(List.of("x"), List.copyOf(NodeLookup.candidates("x", null)), "no normalised form");
    }

    @Test
    @SuppressWarnings("unchecked")
    void everyFormGoesToPostgresAndTheAnswerKeepsTheOrderGiven() {
        var brca1 = row("hgnc:1100", "BRCA1", List.of("hgnc:1100", "ensembl:ENSG00000012048"));
        var psoriasis = row("mondo:0005083", "psoriasis", List.of("mondo:0005083", "doid:8893"));
        when(postgres.lookupNodes(eq("g1"), any(), isNull(), anyInt())).thenReturn(List.of(brca1, psoriasis));

        var answer = lookup.lookup("g1", List.of(IRI, "nope:1", "HGNC:1100"), false, false, null);

        var results = (List<Map<String, Object>>) answer.get("results");
        assertEquals(List.of(IRI, "nope:1", "HGNC:1100"), results.stream().map(r -> r.get("id")).toList());
        assertEquals(List.of(psoriasis), results.get(0).get("nodes"), "the IRI was normalised to the node's curie");
        assertEquals(List.of(), results.get(1).get("nodes"));
        assertEquals(List.of(brca1), results.get(2).get("nodes"), "prefix case does not matter");
        assertEquals(List.of("nope:1"), answer.get("notFound"));
        assertEquals(false, answer.get("truncated"));

        // a row fetched for one identifier is not a match for another whose forms differ from it in case only:
        // whether an identifier matches must not depend on what else was looked up with it
        var alone = lookup.lookup("g1", List.of("MONDO:0005083", "http://example.org/a"), false, false, null);
        assertEquals(List.of("http://example.org/a"), alone.get("notFound"));

        var forms = ArgumentCaptor.forClass(Collection.class);
        verify(postgres).lookupNodes(eq("g1"), forms.capture(), isNull(), eq(31));
        assertEquals(Set.of(IRI, IRI.toLowerCase(), "mondo:0005083", "nope:1", "HGNC:1100", "hgnc:1100"), Set.copyOf(forms.getValue()));
        verify(pgClient, never()).resolveToMap(any(), any());
    }

    @Test
    void namesMatchOnlyWhenAskedFor() {
        var psoriasis = row("mondo:0005083", "Psoriasis", List.of("mondo:0005083"));
        when(postgres.lookupNodes(eq("g1"), any(), any(), anyInt())).thenReturn(List.of(psoriasis));

        var byName = lookup.lookup("g1", List.of("PSORIASIS", "Psoriasis"), true, false, null);
        assertEquals(List.of(), byName.get("notFound"));
        verify(postgres).lookupNodes(eq("g1"), any(), eq(List.of("psoriasis")), eq(21));

        var byIdOnly = lookup.lookup("g1", List.of("PSORIASIS"), false, false, null);
        assertEquals(List.of("PSORIASIS"), byIdOnly.get("notFound"), "a row that only shares its name is not a match");
    }

    @Test
    @SuppressWarnings("unchecked")
    void resolvedHitsAreTheFullNodesInTheLanguageAsked() {
        var hit = row("mondo:0005083", "psoriasis", List.of("mondo:0005083"));
        when(postgres.lookupNodes(eq("g1"), any(), isNull(), anyInt())).thenReturn(List.of(hit));
        when(postgres.getPgClient()).thenReturn(pgClient);
        when(pgClient.resolveToMap(eq("g1"), any())).thenReturn(Map.of("mondo:0005083", GrebiApiNodeRoutesTest.translatedNode()));

        var answer = lookup.lookup("g1", List.of("mondo:0005083"), false, true, "fr");

        var node = ((List<Map<String, Object>>) ((List<Map<String, Object>>) answer.get("results")).get(0).get("nodes")).get(0);
        assertEquals(List.of("en", "de", "fr", "ja"), node.get("grebi:languages"));
        var names = (List<Map<String, Object>>) node.get("grebi:name");
        assertEquals(2, names.size(), "French first, English as the fallback, Japanese dropped");
        assertEquals("psoriasis (fr)", ((Map<String, Object>) names.get(0).get("grebi:value")).get("grebi:value"));
        verify(pgClient).resolveToMap("g1", Set.of("mondo:0005083"));
    }

    @Test
    void hittingTheRowLimitIsReported() {
        var rows = new ArrayList<Map<String, Object>>();
        for (int i = 0; i <= NodeLookup.ROWS_PER_ID; i++) {
            rows.add(row("x:" + i, "x", List.of("x:1")));
        }
        when(postgres.lookupNodes(eq("g1"), any(), isNull(), eq(NodeLookup.ROWS_PER_ID + 1))).thenReturn(rows);

        var answer = lookup.lookup("g1", List.of("x:1"), false, false, null);
        assertEquals(true, answer.get("truncated"));
        assertEquals(NodeLookup.ROWS_PER_ID, ((List<?>) ((Map<?, ?>) ((List<?>) answer.get("results")).get(0)).get("nodes")).size());
    }

    @Test
    void emptyOversizedAndTooManyIdentifiersAreRejectedBeforePostgres() {
        assertEquals(400, assertThrows(ResourceLimits.ResourceLimitException.class,
            () -> lookup.lookup("g1", List.of(), false, false, null)).statusCode());
        var tooMany = assertThrows(ResourceLimits.ResourceLimitException.class,
            () -> lookup.lookup("g1", List.of("a", "b", "c", "d"), false, false, null));
        assertTrue(tooMany.getMessage().contains("3"), tooMany.getMessage());
        assertThrows(ResourceLimits.ResourceLimitException.class,
            () -> lookup.lookup("g1", List.of("x".repeat(65)), false, false, null));
        verify(postgres, never()).lookupNodes(any(), any(), any(), anyInt());
    }

    @Test
    void aFailingPrefixServiceLeavesTheIdentifiersAsGiven() {
        var stubborn = new NodeLookup(postgres, limits, ids -> { throw new RuntimeException("no prefix map"); });
        when(postgres.lookupNodes(eq("g1"), any(), isNull(), anyInt())).thenReturn(List.of());
        // the production normaliser swallows the failure; a normaliser that fails outright is a bug of the caller's
        assertThrows(RuntimeException.class, () -> stubborn.lookup("g1", List.of("a"), false, false, null));

        var shortList = new NodeLookup(postgres, limits, ids -> List.of());
        assertEquals(List.of("a"), shortList.lookup("g1", List.of("a"), false, false, null).get("notFound"),
            "a normaliser that answers the wrong number of forms is ignored");
    }
}
