package uk.ac.ebi.grebi.repo;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.neo4j.driver.EagerResult;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import uk.ac.ebi.grebi.GrebiApi;
import uk.ac.ebi.grebi.db.Neo4jClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.neo4j.driver.Values.parameters;

public class GrebiNeoRepo {

    Neo4jClient neo4jClient = new Neo4jClient();
    Gson gson = new Gson();

    public GrebiNeoRepo() throws IOException {}

    final String STATS_QUERY = new String(GrebiApi.class.getResourceAsStream("/cypher/stats.cypher").readAllBytes(), StandardCharsets.UTF_8);
    final String SEARCH_QUERY = new String(GrebiApi.class.getResourceAsStream("/cypher/search.cypher").readAllBytes(), StandardCharsets.UTF_8);
    final String PROPS_QUERY = new String(GrebiApi.class.getResourceAsStream("/cypher/props.cypher").readAllBytes(), StandardCharsets.UTF_8);

    public Map<String,JsonElement> getEdgeTypes() {
        EagerResult props_res = neo4jClient.getDriver().executableQuery(PROPS_QUERY).withConfig(QueryConfig.builder().withDatabase("neo4j").build()).execute();
        Map<String,JsonElement> edgeTypes = new TreeMap<>();
        for(var r : props_res.records().get(0).values()) {
            System.out.println(r.asString());
            JsonObject prop_def = gson.fromJson(r.asString(), JsonElement.class).getAsJsonObject();
            edgeTypes.put(prop_def.get("grebi:nodeId").getAsString(), prop_def);
        }
        return edgeTypes;
    }

    public Map<String,Object> getStats() {
        EagerResult props_res = neo4jClient.getDriver().executableQuery(STATS_QUERY).withConfig(QueryConfig.builder().withDatabase("neo4j").build()).execute();
        return props_res.records().get(0).values().get(0).asMap();
    }
    public List<Record> cypher(String query, String resVar) {
        EagerResult res = neo4jClient.getDriver().executableQuery(query).withConfig(QueryConfig.builder().withDatabase("neo4j").build()).execute();
        return List.of();
    }

    static Map<String, Object> mapValue(Value value) {
        Map<String, Object> res = new TreeMap<>(value.asMap());
        res.put("grebi:type", StreamSupport.stream(value.asNode().labels().spliterator(), false).collect(Collectors.toList()));
        return res;
    }


}
