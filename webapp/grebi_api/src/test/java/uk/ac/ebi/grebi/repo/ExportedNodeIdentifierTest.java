package uk.ac.ebi.grebi.repo;

import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The identifier an export gives a node. The dataload names the nodes of the
 * tables it publishes by the same rules (grebi_shared, query_results.rs), so a
 * query's CSV and its table in a release agree.
 */
class ExportedNodeIdentifierTest {

    // type 2 diabetes as the graph has it: a disease and a phenotype in one node
    static final List<String> T2D = List.of("type 2 diabetes mellitus", "hp:0005978", "mondo:0005148", "efo:0001360", "doid:9352");

    @Test
    void aNodeIsNamedByWhatItsColumnIsAbout() {
        assertEquals("mondo:0005148", GrebiCypherRepo.pickSourceId(List.of("mondo:"), T2D, "doid:9352"));
        assertEquals("efo:0001360", GrebiCypherRepo.pickSourceId(List.of("orpha:", "efo:"), T2D, "doid:9352"),
            "the first prefix the node has an identifier for");
    }

    @Test
    void withoutAWishOrAnIdentifierForItThePreferredPrefixesDecide() {
        assertEquals("hp:0005978", GrebiCypherRepo.pickSourceId(null, T2D, "doid:9352"));
        assertEquals("hp:0005978", GrebiCypherRepo.pickSourceId(List.of(), T2D, "doid:9352"));
        assertEquals("hp:0005978", GrebiCypherRepo.pickSourceId(List.of("hgnc:"), T2D, "doid:9352"));
    }

    @Test
    void identifiersWithANameAttachedArePassedOver() {
        assertEquals("chebi:16393", GrebiCypherRepo.pickSourceId(null,
            List.of("sphingosine", "chebi:16393 SPHINGOSINE", "mesh:D013110", "chebi:16393"), "inchikey:WWUZIQQURGPMPG-KRWOKUGFSA-N"));
    }

    @Test
    void theNodeIdIsTheLastResortAndNeverAName() {
        assertEquals("go:0070527", GrebiCypherRepo.pickSourceId(null, List.of("platelet aggregation", "go:0070527"), "go:0070527"));
        assertEquals("rs429358", GrebiCypherRepo.pickSourceId(List.of("mondo:"), null, "rs429358"));
        assertNull(GrebiCypherRepo.pickSourceId(null, null, null));
    }

    @Test
    void resultRowsPrefixNodeIdsWithTheirGraph() {
        assertEquals("go:0070527", GrebiCypherRepo.bareNodeId("g1", "g1:go:0070527"));
        assertEquals("go:0070527", GrebiCypherRepo.bareNodeId("g1", "go:0070527"));
        assertEquals("g2:go:0070527", GrebiCypherRepo.bareNodeId("g1", "g2:go:0070527"));
        assertNull(GrebiCypherRepo.bareNodeId("g1", null));
    }

    @Test
    void aCsvRowNamesEachNodeAsItsColumnAsks() {
        var disease = column("disease", "GraphNodeId");
        disease.id_prefixes = List.of("mondo:");
        var phenotype = column("phenotype", "GraphNodeId");
        phenotype.id_prefixes = List.of("hp:");
        var trait = column("trait", "GraphNodeId");
        var columns = List.of(disease, phenotype, trait, column("p_value", "float"), column("edge_id", "EdgeId"));

        Map<String, Object> node = new LinkedHashMap<>();
        node.put("id", T2D);
        node.put("grebi:nodeId", "g1:doid:9352");
        node.put("grebi:name", List.of("type 2 diabetes mellitus"));
        Map<String, Object> unnamed = new LinkedHashMap<>();
        unnamed.put("id", List.of("platelet aggregation", "go:0070527"));
        unnamed.put("grebi:nodeId", "g1:go:0070527");

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("disease", node);
        row.put("phenotype", node);
        row.put("trait", unnamed);
        row.put("p_value", "1E-7");
        row.put("edge_id", "g1:abc");

        assertEquals(List.of("disease_id", "disease_label", "phenotype_id", "phenotype_label", "trait_id", "trait_label", "p_value"),
            GrebiCypherRepo.csvHeader(columns));
        var out = new StringWriter();
        GrebiCypherRepo.writeCsvRow("g1", columns, row, new PrintWriter(out, true));
        assertEquals("\"mondo:0005148\",\"type 2 diabetes mellitus\",\"hp:0005978\",\"type 2 diabetes mellitus\","
            + "\"go:0070527\",\"go:0070527\",\"1.0E-7\"\n", out.toString());
    }

    private static QueryTemplate.ResultColumn column(String id, String type) {
        var c = new QueryTemplate.ResultColumn();
        c.column_id = id;
        c.column_type = type;
        return c;
    }
}
