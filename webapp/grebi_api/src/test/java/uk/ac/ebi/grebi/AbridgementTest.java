package uk.ac.ebi.grebi;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
    @SuppressWarnings("unchecked")
    void theNodesThatNameThePropertiesAreKeptOnlyWhenAsked() {
        var node = hub(30);
        ((Map<String, Object>) node.get("_refs")).put("biolink:interacts_with", Map.of("grebi:name", "interacts with"));

        var forAnAgent = (Map<String, Object>) Abridgement.abridge(node, 20).get("_refs");
        assertEquals(20, forAnAgent.size());
        assertFalse(forAnAgent.containsKey("biolink:interacts_with"), "an agent reads the property name itself");

        var forAPage = (Map<String, Object>) Abridgement.abridge(node, 20, true).get("_refs");
        assertEquals(21, forAPage.size());
        assertEquals(Map.of("grebi:name", "interacts with"), forAPage.get("biolink:interacts_with"), "a page labels the property with it");

        var read = (Map<String, Object>) readAsTheWholeNodeGives(blob(node), 20, null).get("_refs");
        assertTrue(read.containsKey("biolink:interacts_with"), "and a stored node read abridged keeps them");
        assertTrue(readInOnePass(blob(node), 20, null));
    }

    @Test
    void nullAndNonsenseLimits() {
        assertNull(Abridgement.abridge(null, 20));
        assertThrows(IllegalArgumentException.class, () -> Abridgement.abridge(hub(3), 0));
    }

    // --- Reading a stored node abridged, without holding it whole ---

    static final Gson GSON = new GsonBuilder().serializeNulls().create();

    /** A node as it is stored: its JSON, deflated. */
    static byte[] blob(Object node) {
        var bytes = new ByteArrayOutputStream();
        try (var out = new OutputStreamWriter(new DeflaterOutputStream(bytes), StandardCharsets.UTF_8)) {
            GSON.toJson(node, out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    /** What the whole node gives: read in full, put in the language asked for, then abridged, keeping the nodes that name its properties. */
    static Map<String, Object> fromTheWholeNode(byte[] blob, int maxValues, String lang) {
        try (var reader = new InputStreamReader(new InflaterInputStream(new ByteArrayInputStream(blob)), StandardCharsets.UTF_8)) {
            Map<String, Object> node = GSON.fromJson(reader, new TypeToken<Map<String, Object>>() {}.getType());
            return Abridgement.abridge(Languages.localise(node, lang), maxValues, true);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Reads the blob abridged and checks it is what the whole node gives, down to the order of its properties. */
    static Map<String, Object> readAsTheWholeNodeGives(byte[] blob, int maxValues, String lang) {
        var expected = fromTheWholeNode(blob, maxValues, lang);
        var read = Abridgement.read(blob, maxValues, lang);
        var what = "maxValues " + maxValues + ", lang " + lang;
        assertEquals(expected, read, what);
        if (expected != null) {
            assertEquals(List.copyOf(expected.keySet()), List.copyOf(read.keySet()), "the order of the properties, " + what);
        }
        return read;
    }

    static boolean readInOnePass(byte[] blob, int maxValues, String lang) {
        try {
            return Abridgement.readStreamed(blob, maxValues, lang) != null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void aStoredHubNodeIsReadAbridgedInOnePass() {
        var blob = blob(hub(30));
        var read = readAsTheWholeNodeGives(blob, 20, null);
        assertTrue(readInOnePass(blob, 20, null));
        assertEquals(20, ((List<Object>) read.get("biolink:interacts_with")).size());
        assertEquals(Map.of("biolink:interacts_with", 30), read.get(Abridgement.TRUNCATED));
        var refs = (Map<String, Object>) read.get("_refs");
        assertEquals(20, refs.size(), "only the nodes the kept values mention");
        assertFalse(refs.containsKey("n20"));
        assertFalse(refs.containsKey("self"));

        // within the limit nothing is cut, and every referenced node stays
        var whole = readAsTheWholeNodeGives(blob, 30, "fr");
        assertTrue(readInOnePass(blob, 30, "fr"));
        assertFalse(whole.containsKey(Abridgement.TRUNCATED));
        assertEquals(31, ((Map<String, Object>) whole.get("_refs")).size());
    }

    @Test
    void aStoredNodeLeavesOutItsVectorsAndAnyEarlierCount() {
        var node = hub(3);
        node.remove("_refs");
        node.put("embedding:text-embedding-3-small", List.of(0.1, 0.2, 0.3, 0.4));
        node.put(Abridgement.TRUNCATED, Map.of("stale", 99));
        node.put("a number", 7);
        node.put("nothing", null);
        node.put("nested", Map.of("grebi:value", List.of("n1", Map.of("deeper", "n2"))));
        var blob = blob(node);
        var read = readAsTheWholeNodeGives(blob, 2, null);
        assertTrue(readInOnePass(blob, 2, null));
        assertEquals(List.of("grebi:nodeId", "grebi:name", "biolink:interacts_with", "grebi:curie", "a number", "nothing", "nested", Abridgement.TRUNCATED),
                List.copyOf(read.keySet()));
        assertEquals(Map.of("biolink:interacts_with", 3), read.get(Abridgement.TRUNCATED));
        assertNull(read.get("nothing"));
    }

    /** A hub node with its name in three languages and a synonym in a fourth. */
    static Map<String, Object> translatedHub(int n) {
        var node = hub(n);
        node.put("grebi:name", List.of(LanguagesTest.merged("psoriasis"), LanguagesTest.merged(LanguagesTest.translated("psoriasis (fr)", "fr")),
                LanguagesTest.merged(LanguagesTest.translated("乾癬", "ja"))));
        node.put("grebi:synonym", List.of(LanguagesTest.merged(LanguagesTest.translated("Schuppenflechte", "de"))));
        // the referenced nodes come last, as the dataload writes them
        node.put("_refs", node.remove("_refs"));
        return node;
    }

    @Test
    @SuppressWarnings("unchecked")
    void aTranslatedHubNodeIsReadInOnePassWhileItsTranslatedPropertiesAreWhole() {
        var blob = blob(translatedHub(30));
        for (var lang : Arrays.asList(null, "fr", "ja", "en", "xx")) {
            var read = readAsTheWholeNodeGives(blob, 5, lang);
            assertTrue(readInOnePass(blob, 5, lang), "lang " + lang);
            assertEquals(List.of("en", "de", "fr", "ja"), read.get(Languages.LANGUAGES));
            assertEquals(Map.of("biolink:interacts_with", 30), read.get(Abridgement.TRUNCATED));
        }
        var french = readAsTheWholeNodeGives(blob, 5, "fr");
        assertEquals(2, ((List<Object>) french.get("grebi:name")).size(), "the French name, then the English one");
        assertFalse(french.containsKey("grebi:synonym"), "a property only in other languages is dropped");
    }

    @Test
    void aPropertyCutShortThatHasLanguagesNeedsTheWholeNode() {
        var blob = blob(translatedHub(30));
        // the name has three values: cut to two, the one left out is in a language
        assertFalse(readInOnePass(blob, 2, null), "the node's languages are those of all its values");
        readAsTheWholeNodeGives(blob, 2, null);
        // cut to one, the name kept is English but the ones a language would bring forward are not read
        assertFalse(readInOnePass(blob, 1, "fr"));
        readAsTheWholeNodeGives(blob, 1, "fr");
        readAsTheWholeNodeGives(blob, 2, "ja");

        // a language among the values kept, none past the cut
        var node = hub(3);
        node.put("grebi:name", List.of(LanguagesTest.merged(LanguagesTest.translated("nom", "fr")), LanguagesTest.merged("name"), LanguagesTest.merged("another name")));
        var kept = blob(node);
        assertTrue(readInOnePass(kept, 2, null), "without a language asked for, the values keep their places");
        assertFalse(readInOnePass(kept, 2, "ja"), "asking for a language drops the French name and brings the third forward");
        for (var lang : Arrays.asList(null, "fr", "ja")) {
            readAsTheWholeNodeGives(kept, 2, lang);
        }
    }

    @Test
    void aPropertyAfterTheReferencedNodesNeedsTheWholeNode() {
        var node = hub(30);
        node.put("biolink:after", List.of("n25", "n26", "n27"));
        var blob = blob(node);
        assertFalse(readInOnePass(blob, 20, null), "the referenced nodes were chosen before n25 was mentioned");
        var read = readAsTheWholeNodeGives(blob, 20, null);
        assertTrue(((Map<?, ?>) read.get("_refs")).containsKey("n25"));

        // a vector after them is left out either way, so it does not matter
        var vectorLast = hub(30);
        vectorLast.put("embedding:m", List.of(0.5));
        assertTrue(readInOnePass(blob(vectorLast), 20, null));
        readAsTheWholeNodeGives(blob(vectorLast), 20, null);
    }

    @Test
    void whatIsNotANodeIsReadAsTheWholeNodeWouldBe() {
        assertNull(Abridgement.read(null, 20, null), "no blob, no node");
        assertNull(readAsTheWholeNodeGives(blob(null), 20, "fr"));
        assertThrows(IllegalArgumentException.class, () -> Abridgement.read(blob(hub(3)), 0, null));

        var odd = hub(3);
        odd.put("_refs", "not a map");
        assertFalse(readAsTheWholeNodeGives(blob(odd), 2, null).containsKey("_refs"));
    }

    @Test
    void anyNodeIsReadAsTheWholeNodeGives() {
        var random = new Random(92);
        var languages = Arrays.asList(null, "", "fr", "ja", "en", "de");
        var onePass = 0;
        for (int i = 0; i < 3000; i++) {
            var blob = blob(randomNode(random));
            var maxValues = 1 + random.nextInt(5);
            var lang = languages.get(random.nextInt(languages.size()));
            readAsTheWholeNodeGives(blob, maxValues, lang);
            if (readInOnePass(blob, maxValues, lang)) {
                onePass++;
            }
        }
        assertTrue(onePass > 500 && onePass < 2500, "both ways of reading were exercised: " + onePass + " of 3000 in one pass");
    }

    /** A node of a few properties with values of every shape, some in languages, and referenced nodes in or out of place. */
    static Map<String, Object> randomNode(Random random) {
        var keys = List.of("grebi:name", "grebi:synonym", "biolink:affects", "biolink:part_of", "ex:p", "embedding:m", Abridgement.TRUNCATED);
        var node = new LinkedHashMap<String, Object>();
        node.put("grebi:nodeId", "n0");
        var refsAt = random.nextInt(10) < 7 ? -1 : random.nextInt(5);
        var properties = 1 + random.nextInt(5);
        for (int p = 0; p < properties; p++) {
            if (p == refsAt) {
                node.put("_refs", randomRefs(random));
            }
            var key = keys.get(random.nextInt(keys.size()));
            if (random.nextInt(6) == 0) {
                node.put(key, randomValue(random, false));
            } else {
                var values = new ArrayList<Object>();
                for (int v = random.nextInt(9); v > 0; v--) {
                    values.add(randomValue(random, random.nextInt(4) == 0));
                }
                node.put(key, values);
            }
        }
        if (refsAt < 0 && random.nextInt(5) > 0) {
            node.put("_refs", randomRefs(random));
        }
        return node;
    }

    static Map<String, Object> randomRefs(Random random) {
        var refs = new LinkedHashMap<String, Object>();
        for (int i = 0; i < 10; i++) {
            if (random.nextBoolean()) {
                refs.put("n" + i, Map.of("grebi:name", List.of("node " + i)));
            }
        }
        // some properties are named by nodes
        for (var property : List.of("biolink:affects", "ex:p", "grebi:languages")) {
            if (random.nextBoolean()) {
                refs.put(property, Map.of("grebi:name", List.of("the property " + property)));
            }
        }
        return refs;
    }

    static Object randomValue(Random random, boolean inALanguage) {
        var id = "n" + random.nextInt(10);
        if (inALanguage) {
            var translated = LanguagesTest.translated("mot " + id, List.of("fr", "ja", "de").get(random.nextInt(3)));
            return random.nextBoolean() ? LanguagesTest.merged(translated) : translated;
        }
        return switch (random.nextInt(7)) {
            case 0 -> id;
            case 1 -> LanguagesTest.merged(id);
            case 2 -> LanguagesTest.merged(Map.of("grebi:value", id, "grebi:properties", Map.of("ex:evidence", List.of("n" + random.nextInt(10)))));
            case 3 -> random.nextInt(100);
            case 4 -> List.of(id, List.of("n" + random.nextInt(10)));
            case 5 -> Map.of("ex:about", id);
            default -> random.nextBoolean();
        };
    }
}
