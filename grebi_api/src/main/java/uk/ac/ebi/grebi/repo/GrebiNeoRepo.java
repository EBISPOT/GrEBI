package uk.ac.ebi.grebi.repo;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.neo4j.driver.EagerResult;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.Value;
import org.springframework.data.domain.Pageable;
import uk.ac.ebi.grebi.GrebiApi;
import uk.ac.ebi.grebi.db.Neo4jClient;
import uk.ac.ebi.grebi.db.ResolverClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GrebiNeoRepo {

    Neo4jClient neo4jClient = new Neo4jClient();
    ResolverClient resolver = new ResolverClient();
    Gson gson = new Gson();

    public GrebiNeoRepo() throws IOException {}

    final String STATS_QUERY = new String(GrebiApi.class.getResourceAsStream("/cypher/stats.cypher").readAllBytes(), StandardCharsets.UTF_8);
    final String INCOMING_EDGES_QUERY = new String(GrebiApi.class.getResourceAsStream("/cypher/incoming_edges.cypher").readAllBytes(), StandardCharsets.UTF_8);

    public Map<String,Object> getStats() {
        EagerResult props_res = neo4jClient.getDriver().executableQuery(STATS_QUERY).withConfig(QueryConfig.builder().withDatabase("neo4j").build()).execute();
        return props_res.records().get(0).values().get(0).asMap();
    }
    public List<Record> cypher(String query, String resVar) {
        EagerResult res = neo4jClient.getDriver().executableQuery(query).withConfig(QueryConfig.builder().withDatabase("neo4j").build()).execute();
        return List.of();
    }

    public class EdgeAndNode {
        public Map<String,JsonElement> edge, node;
        public EdgeAndNode(Map<String,JsonElement> edge, Map<String,JsonElement> node) {
            this.edge = edge;
            this.node = node;
        }
    }

    public List<EdgeAndNode> getIncomingEdges(String subgraph, String nodeId, Pageable pageable) {
        EagerResult res = neo4jClient.getDriver().executableQuery(INCOMING_EDGES_QUERY)
            .withParameters(Map.of(
                    "nodeId", subgraph + ":" + nodeId,
                    "offset", pageable.getOffset(),
                    "limit", pageable.getPageSize()
            ))
            .withConfig(QueryConfig.builder().withDatabase("neo4j").build()).execute();

        var resolved = resolver.resolveToMap(
                subgraph,
                res.records().stream().flatMap(record -> {
                    var props = record.asMap();
                    return List.of(
                            removeSubgraphPrefix((String) props.get("otherId"), subgraph),
                            removeSubgraphPrefix((String) props.get("edgeId"), subgraph)
                    ).stream();
                }).collect(Collectors.toSet()));

        return res.records().stream().map(record -> {
            var props = record.asMap();
            var otherId = (String)props.get("otherId");
            var edgeId = (String)props.get("edgeId");
            return new EdgeAndNode(resolved.get(edgeId), resolved.get(otherId));
        }).collect(Collectors.toList());
    }

    private String removeSubgraphPrefix(String id, String subgraph) {
        if(!id.startsWith(subgraph + ":")) {
            throw new RuntimeException();
        }
        return id.substring(subgraph.length() + 1);
    }

    static Map<String, Object> mapValue(Value value) {
        Map<String, Object> res = new TreeMap<>(value.asMap());
        res.put("grebi:type", StreamSupport.stream(value.asNode().labels().spliterator(), false).collect(Collectors.toList()));
        return res;
    }


}
