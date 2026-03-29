package uk.ac.ebi.grebi.repo;

import java.util.Map;
import java.util.Set;

import com.google.gson.JsonElement;

import uk.ac.ebi.grebi.db.MetadataClient;

public class GrebiMetadataRepo {

    Map<String,JsonElement> graph2metadata;
    
    public GrebiMetadataRepo() {

        MetadataClient MetadataClient = new MetadataClient();
        graph2metadata = MetadataClient.getMetadatas();

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
