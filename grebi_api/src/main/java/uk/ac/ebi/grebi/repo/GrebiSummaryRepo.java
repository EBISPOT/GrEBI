package uk.ac.ebi.grebi.repo;

import java.util.Map;
import java.util.Set;

import com.google.gson.JsonElement;

import uk.ac.ebi.grebi.db.SummaryClient;

public class GrebiSummaryRepo {

    Map<String,JsonElement> subgraph2summary;
    
    public GrebiSummaryRepo() {

        SummaryClient summaryClient = new SummaryClient();
        subgraph2summary = summaryClient.getSummaries();

    }

    public Set<String> getSubgraphs() {
        return subgraph2summary.keySet();
    }

    public Map<String,JsonElement> getSummary(String subgraph) {
        return subgraph2summary.get(subgraph).getAsJsonObject().asMap();
    }

    public Set<String> getAllEdgeProps(String subgraph) {
        return getSummary(subgraph).get("edge_props").getAsJsonObject().keySet();
    }


}
