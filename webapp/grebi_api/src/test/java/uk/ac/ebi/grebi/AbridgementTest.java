package uk.ac.ebi.grebi;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AbridgementTest {

    /** A node with a property of n values n0..n(n-1), each a reference to a node of the same id. */
    static Map<String, Object> hub(int n) {
        var values = new ArrayList<Object>();
        var refs = new LinkedHashMap<String, Object>();
        for (int i = 0; i < n; i++) {
            // a reified value mentions its node inside grebi:value, as the real blobs do
            values.add(Map.of("grebi:datasources", List.of("STRING"), "grebi:value", Map.of("grebi:value", "n" + i)));
            refs.put("n" + i, Map.of("grebi:name", "node " + i));
        }
        refs.put("self", Map.of("grebi:name", "the node itself, which nothing mentions"));
        var node = new LinkedHashMap<String, Object>();
        node.put("grebi:nodeId", "hub");
        node.put("grebi:name", List.of("APOE"));
        node.put("biolink:interacts_with", values);
        node.put("grebi:curie", "hub");
        node.put("_refs", refs);
        return node;
    }

    @Test
    void embeddingVectorsAreLeftOut() {
        var node = hub(3);
        var vector = new ArrayList<Object>();
        for (int i = 0; i < 512; i++) {
            vector.add(i * 0.01);
        }
        node.put("embedding:text-embedding-3-small", vector);
        var abridged = Abridgement.abridge(node, 20);
        assertFalse(abridged.containsKey("embedding:text-embedding-3-small"), "a vector cut short is no vector at all");
        assertFalse(abridged.containsKey(Abridgement.TRUNCATED), "and it is not counted as a cut property");
        assertEquals(List.of("grebi:nodeId", "grebi:name", "biolink:interacts_with", "grebi:curie", "_refs"), List.copyOf(abridged.keySet()));
    }

    @Test
    void aNodeWithinTheLimitIsCopiedWhole() {
        var node = hub(3);
        var abridged = Abridgement.abridge(node, 3);
        assertEquals(node, abridged);
        assertFalse(abridged.containsKey(Abridgement.TRUNCATED));
        assertEquals(List.copyOf(node.keySet()), List.copyOf(abridged.keySet()), "the properties keep their order");
    }

    @Test
    @SuppressWarnings("unchecked")
    void aLongPropertyKeepsItsFirstValuesAndSaysHowManyThereAre() {
        var node = hub(30);
        var abridged = Abridgement.abridge(node, 20);

        var kept = (List<Object>) abridged.get("biolink:interacts_with");
        assertEquals(20, kept.size());
        assertEquals(((List<Object>) node.get("biolink:interacts_with")).subList(0, 20), kept);
        assertEquals(Map.of("biolink:interacts_with", 30), abridged.get(Abridgement.TRUNCATED));
        assertEquals(List.of("APOE"), abridged.get("grebi:name"), "a short property is untouched");
        assertEquals(List.of("grebi:nodeId", "grebi:name", "biolink:interacts_with", "grebi:curie", "_refs", Abridgement.TRUNCATED),
                List.copyOf(abridged.keySet()));

        var refs = (Map<String, Object>) abridged.get("_refs");
        assertEquals(20, refs.size(), "only the nodes the kept values mention");
        assertTrue(refs.containsKey("n0") && refs.containsKey("n19"));
        assertFalse(refs.containsKey("n20"), "a node only a cut value mentioned");
        assertFalse(refs.containsKey("self"), "a node nothing mentions");

        assertEquals(30, ((List<Object>) node.get("biolink:interacts_with")).size(), "the node itself is left alone");
        assertEquals(31, ((Map<String, Object>) node.get("_refs")).size());
    }

    @Test
    @SuppressWarnings("unchecked")
    void anEarlierAbridgementIsRecomputed() {
        var once = Abridgement.abridge(hub(30), 20);
        var again = Abridgement.abridge(once, 5);
        assertEquals(5, ((List<Object>) again.get("biolink:interacts_with")).size());
        assertEquals(Map.of("biolink:interacts_with", 20), again.get(Abridgement.TRUNCATED),
                "the count is of the values it was given, not a stale one carried over");
        assertEquals(5, ((Map<String, Object>) again.get("_refs")).size());

        var whole = Abridgement.abridge(once, 20);
        assertFalse(whole.containsKey(Abridgement.TRUNCATED), "a node already within the limit is not marked");
    }

    @Test
    void nullAndNonsenseLimits() {
        assertNull(Abridgement.abridge(null, 20));
        assertThrows(IllegalArgumentException.class, () -> Abridgement.abridge(hub(3), 0));
    }
}
