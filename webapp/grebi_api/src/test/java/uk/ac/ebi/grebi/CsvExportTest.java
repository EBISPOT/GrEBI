package uk.ac.ebi.grebi;

import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** CSV cells, rows and the page-by-page streaming. */
class CsvExportTest {

    @Test
    void cellsJoinListsUnwrapMergedValuesAndSpellNumbersPlainly() {
        assertEquals("", CsvExport.cell(null));
        assertEquals("a; b", CsvExport.cell(List.of("a", "b")));
        assertEquals("psoriasis", CsvExport.cell(Map.of("grebi:datasources", List.of("A"), "grebi:sourceIds", List.of(), "grebi:value", "psoriasis")));
        assertEquals("Gène", CsvExport.cell(Map.of("grebi:value", "Gène", "grebi:properties", Map.of("grebi:lang", List.of("fr")))));
        assertEquals("{\"k\":\"v\"}", CsvExport.cell(Map.of("k", "v")));
        assertEquals("3", CsvExport.cell(3.0));
        assertEquals("0.5", CsvExport.cell(0.5));
        assertEquals("true", CsvExport.cell(true));
    }

    @Test
    void rowsQuoteWhatNeedsQuoting() {
        assertEquals("a,b\n", CsvExport.row(List.of("a", "b")));
        assertEquals("\"Smith, \"\"Jr\"\"\",\"two\nlines\",plain\n", CsvExport.row(List.of("Smith, \"Jr\"", "two\nlines", "plain")));
        assertEquals("mondo_0005083_incoming_edges.csv", CsvExport.fileName("mondo:0005083", "_incoming_edges.csv"));
        assertEquals("http_example.org_A.csv", CsvExport.fileName("http://example.org/A", ".csv"));
    }

    @Test
    void nodesStreamPageByPageUntilAShortPage() throws Exception {
        var asked = new ArrayList<Integer>();
        var out = new StringWriter();
        CsvExport.writeNodes(out, page -> {
            asked.add(page);
            var rows = new ArrayList<Map<String, Object>>();
            int count = page == 0 ? CsvExport.PAGE_SIZE : 1;
            for (int i = 0; i < count; i++) {
                rows.add(Map.of("grebi:nodeId", "n" + page + "_" + i, "grebi:name", "Node, " + i, "grebi:type", List.of("biolink:Gene", "ols:Class"),
                        "grebi:datasources", List.of("A"), "grebi:sourceIds", List.of("x:" + i), "grebi:curie", "X:" + i));
            }
            return rows;
        });
        var lines = out.toString().split("\n");
        assertEquals(List.of(0, 1), asked, "a full page asks for the next; a short one ends it");
        assertEquals(1 + CsvExport.PAGE_SIZE + 1, lines.length);
        assertEquals("grebi:nodeId,grebi:name,grebi:type,grebi:datasources,grebi:sourceIds,grebi:curie", lines[0]);
        assertEquals("n0_0,\"Node, 0\",biolink:Gene; ols:Class,A,x:0,X:0", lines[1]);
    }

    @Test
    void edgesCarryTheirEndsByNameAndTheRestAsJson() throws Exception {
        var out = new StringWriter();
        CsvExport.writeEdges(out, page -> page > 0 ? List.of() : List.of(Map.of(
                "grebi:edgeId", "e1", "grebi:type", "biolink:has_phenotype", "grebi:fromNodeId", "a", "grebi:toNodeId", "b",
                "grebi:datasources", List.of("HPOA"), "grebi:subgraph", "g",
                "from", Map.of("grebi:nodeId", "a", "grebi:name", List.of("Marfan syndrome")),
                "to", Map.of("grebi:nodeId", "b"),
                "_refs", Map.of(), "hpoa:frequency", List.of("HP:0040282"))));
        assertEquals("grebi:edgeId,grebi:type,grebi:fromNodeId,from,grebi:toNodeId,to,grebi:datasources,properties\n"
                + "e1,biolink:has_phenotype,a,Marfan syndrome,b,b,HPOA,\"{\"\"hpoa:frequency\"\":[\"\"HP:0040282\"\"]}\"\n", out.toString());
    }

    @Test
    void anExportStopsAtTheRowCap() throws Exception {
        var out = new StringWriter();
        var full = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < CsvExport.PAGE_SIZE; i++) {
            full.add(Map.of("grebi:nodeId", "n"));
        }
        CsvExport.writeNodes(out, page -> full);
        assertEquals(1 + CsvExport.MAX_ROWS, out.toString().split("\n").length);
    }
}
