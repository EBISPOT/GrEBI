package uk.ac.ebi.grebi;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Choosing a language among a node's values. */
class LanguagesTest {

    /** A merged property value: the value wrapped with its provenance. */
    static Map<String, Object> merged(Object value) {
        return Map.of("grebi:datasources", List.of("A"), "grebi:sourceIds", List.of("x"), "grebi:value", value);
    }

    /** A translation as the dataload writes it: a reified value carrying grebi:lang. */
    static Map<String, Object> translated(String text, String lang) {
        return Map.of("grebi:value", text, "grebi:properties", Map.of("grebi:lang", List.of(lang)));
    }

    static Map<String, Object> node() {
        var node = new LinkedHashMap<String, Object>();
        node.put("grebi:nodeId", "ex:a");
        node.put("grebi:name", List.of(merged("Gene A"), merged(translated("Gène A", "fr")), merged(translated("遺伝子 A", "ja"))));
        node.put("grebi:synonym", List.of(merged(translated("übersetzter Begriff", "de"))));
        node.put("grebi:description", List.of(merged(Map.of("grebi:value", "cited", "grebi:properties", Map.of("oboinowl:hasDbXref", List.of("PMID:1"))))));
        node.put("grebi:type", List.of(merged("ex:Gene")));
        return node;
    }

    @Test
    void theLanguageOfAValueIsItsTagOrNothing() {
        assertNull(Languages.languageOf(merged("Gene A")));
        assertNull(Languages.languageOf("Gene A"));
        assertNull(Languages.languageOf(merged(Map.of("grebi:value", "x", "grebi:properties", Map.of("p", List.of("q"))))), "reified without a language");
        assertEquals("fr", Languages.languageOf(merged(translated("Gène A", "fr"))));
        assertEquals("fr", Languages.languageOf(translated("Gène A", " FR ")), "bare and normalised");
    }

    @Test
    void aNodeListsItsLanguagesEnglishFirst() {
        assertEquals(List.of("en", "de", "fr", "ja"), Languages.languagesOf(node()));
        assertEquals(List.of(), Languages.languagesOf(Map.of("grebi:name", List.of(merged("only english")))), "nothing translated, nothing listed");
    }

    @Test
    void withoutALanguageTheValuesAreUntouchedButTheLanguagesAreListed() {
        var original = node();
        var localised = Languages.localise(original, null);
        assertEquals(List.of("en", "de", "fr", "ja"), localised.get("grebi:languages"));
        assertEquals(original.get("grebi:name"), localised.get("grebi:name"));
        assertEquals(original.get("grebi:synonym"), localised.get("grebi:synonym"));
        assertFalse(original.containsKey("grebi:languages"), "the input is not modified");
        assertEquals(localised, Languages.localise(original, " "));
    }

    @Test
    void theChosenLanguageComesFirstThenEnglishAndTheRestGo() {
        var localised = Languages.localise(node(), "fr");
        assertEquals(List.of(merged(translated("Gène A", "fr")), merged("Gene A")), localised.get("grebi:name"));
        assertFalse(localised.containsKey("grebi:synonym"), "a property with values only in other languages is dropped");
        assertEquals(node().get("grebi:description"), localised.get("grebi:description"), "reification that is not a translation is untouched");
        assertEquals(node().get("grebi:type"), localised.get("grebi:type"));
        assertEquals(List.of("en", "de", "fr", "ja"), localised.get("grebi:languages"), "listed before the choice narrowed them");
    }

    @Test
    void englishKeepsOnlyTheUntaggedValues() {
        var localised = Languages.localise(node(), "en");
        assertEquals(List.of(merged("Gene A")), localised.get("grebi:name"));
        assertFalse(localised.containsKey("grebi:synonym"));
        assertEquals(List.of(merged("Gene A")), Languages.localise(node(), "EN").get("grebi:name"), "case does not matter");
    }

    @Test
    void anUnknownLanguageFallsBackToEnglish() {
        assertEquals(List.of(merged("Gene A")), Languages.localise(node(), "zu").get("grebi:name"));
    }

    @Test
    void aNodeWithoutTranslationsIsReturnedAsItIs() {
        var node = Map.<String, Object>of("grebi:nodeId", "ex:b", "grebi:name", List.of(merged("B")));
        assertEquals(node, Languages.localise(node, "fr"));
        assertNull(Languages.localise(null, "fr"));
    }
}
