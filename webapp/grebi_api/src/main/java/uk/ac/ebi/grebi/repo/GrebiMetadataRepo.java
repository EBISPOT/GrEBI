package uk.ac.ebi.grebi.repo;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Set;

import com.google.gson.JsonElement;

import uk.ac.ebi.grebi.GraphOrder;
import uk.ac.ebi.grebi.db.GrebiPostgresClient;

public class GrebiMetadataRepo {

    Map<String,JsonElement> graph2metadata;
    
    public GrebiMetadataRepo(GrebiPostgresClient pgClient) {
        var unorderedMetadata = pgClient.getGraphMetadata();
        graph2metadata = new LinkedHashMap<>();
        for (String graph : GraphOrder.orderedSet(unorderedMetadata.keySet())) {
            graph2metadata.put(graph, unorderedMetadata.get(graph));
        }
    }

    public Set<String> getGraphs() {
        return graph2metadata.keySet();
    }

    public Map<String,JsonElement> getMetadata(String graph) {
        return graph2metadata.get(graph).getAsJsonObject().asMap();
    }

    public Set<String> getAllEdgeProps(String graph) {
        return getMetadata(graph).get("edge_props").getAsJsonObject().keySet();
    }


}
