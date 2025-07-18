package uk.ac.ebi.grebi.repo;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;

import org.neo4j.driver.EagerResult;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.Value;
import org.springframework.data.domain.Pageable;
import uk.ac.ebi.grebi.GrebiApi;
import uk.ac.ebi.grebi.db.Neo4jClient;
import uk.ac.ebi.grebi.db.ResolverClient;
import uk.ac.ebi.grebi.repo.QueryTemplate;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GrebiNeoRepo {

    public static String[] getNeo4jHosts() {
        var env = System.getenv("GREBI_NEO4J_HOSTS");
        if(env != null)
            return env.split(";");
        else
            return List.of("bolt://localhost:7687/").toArray(new String[0]);
    }

    Map<String, Neo4jClient> subgraphToClient = new HashMap<>();

    ResolverClient resolver = new ResolverClient();
    Gson gson = new Gson();

    public GrebiNeoRepo() throws IOException {

        for(String host : getNeo4jHosts()) {
            Neo4jClient client = new Neo4jClient(host);

            String subgraph = (String)
                    client.rawQuery("MATCH (n:GraphNode) RETURN n.`grebi:subgraph` AS subgraph LIMIT 1")
                        .get(0).get("subgraph");

            subgraphToClient.put(subgraph, client);
        }
    }

    private Neo4jClient getClient(String subgraph) {
        var client = subgraphToClient.get(subgraph);
        if(client != null)
            return client;
        throw new IllegalArgumentException("subgraph " + subgraph + " not found");
    }

    public Set<String> getSubgraphs() {
        return subgraphToClient.keySet();
    }

    final String STATS_QUERY = new String(GrebiApi.class.getResourceAsStream("/cypher/stats.cypher").readAllBytes(), StandardCharsets.UTF_8);
    final String INCOMING_EDGES_QUERY = new String(GrebiApi.class.getResourceAsStream("/cypher/incoming_edges.cypher").readAllBytes(), StandardCharsets.UTF_8);

    public Map<String, Map<String,Object>> getStats() {
        Map<String, Map<String,Object>> subgraphToStats = new HashMap<>();
        for(var subgraph : subgraphToClient.keySet()) {
            EagerResult props_res = getClient(subgraph).getDriver().executableQuery(STATS_QUERY).withConfig(QueryConfig.builder().withDatabase("neo4j").build()).execute();
            subgraphToStats.put(subgraph, props_res.records().get(0).values().get(0).asMap());
        }
        return subgraphToStats;
    }

    public class EdgeAndNode {
        public Map<String,Object> edge, node;
        public EdgeAndNode(Map<String,Object> edge, Map<String,Object> node) {
            this.edge = edge;
            this.node = node;
        }
    }

    public List<EdgeAndNode> getIncomingEdges(String subgraph, String nodeId, Pageable pageable) {
        EagerResult res = getClient(subgraph).getDriver().executableQuery(INCOMING_EDGES_QUERY)
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
            var otherId = removeSubgraphPrefix((String)props.get("otherId"), subgraph);
            var edgeId = removeSubgraphPrefix((String)props.get("edgeId"), subgraph);
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

    public Page<Map<String,Object>> runQueryFromTemplate(
        String subgraph, 
        QueryTemplate template,
        Map<String, List<String>> params,
        boolean resolve,
        Pageable pageable
        ) {

        if(!template.subgraphs.contains(subgraph)) {
            throw new IllegalArgumentException("Query template " + template.id + " is not available for subgraph " + subgraph);
        }

        for(var param : params.entrySet()) {
            if(!template.params.stream().anyMatch(p -> p.param_id.equals(param.getKey()))) {
                throw new IllegalArgumentException("Unknown parameter " + param.getKey() + " provided for query template " + template.id + "; valid parameters are: " +
                    template.params.stream().map(p -> p.param_id).collect(Collectors.joining(", ")));
            }
        }

        Map<String, Object> paramMap = new HashMap<>();
        for (QueryTemplate.Parameter p : template.params) {

            var values = params.get(p.param_id);

            if(values == null || values.isEmpty()) {
                if(p.param_default != null) {
                    values = List.of(p.param_default);
                } else {
                    throw new IllegalArgumentException("Parameter " + p.param_id + " is required but not provided");
                }
            }

            if(p.param_type.equals("SourceId")) {
                if(values.size() > 1) {
                    throw new IllegalArgumentException("SourceId param " + p.param_id + " cannot have multiple values");
                }
                var nodeId = values.get(0);
                paramMap.put(p.param_id, nodeId);
            } else if(p.param_type.equals("string")) {

                if(values.size() > 1) {
                    throw new IllegalArgumentException("String param " + p.param_id + " cannot have multiple values");
                }
                var stringValue = values.get(0);
                paramMap.put(p.param_id, stringValue);

            } else if(p.param_type.equals("float")) {

                if(values.size() > 1) {
                    throw new IllegalArgumentException("Float param " + p.param_id + " cannot have multiple values");
                }
                try {
                    var floatValue = Double.parseDouble(values.get(0));
                    paramMap.put(p.param_id, floatValue);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid float value for parameter " + p.param_id + ": " + values.get(0));
                }

            } else {
                throw new IllegalArgumentException("Unknown parameter type " + p.param_type + " for parameter " + p.param_id);
            }
        }

        String query = template.cypher_match_fragment.trim()
                + "\n" + template.cypher_return_fragment.trim();


        if(pageable.getSort() != null && !pageable.getSort().isUnsorted()) {
            var sort = pageable.getSort().stream().collect(Collectors.toList());
            if(sort.size() != 1) {
                throw new IllegalArgumentException("Sorting by multiple columns is not supported");
            }
            var sortField = sort.get(0).getProperty();
            if(!template.result_columns.stream().anyMatch(c -> c.column_id.equals(sortField))) {
                throw new IllegalArgumentException("Sort column " + sortField + " not found; valid columns are: " +
                    template.result_columns.stream().map(c -> c.column_id).collect(Collectors.joining(", ")));
            }
            if(sort.get(0).isAscending()) {
                query += "\nORDER BY " + sortField + " ASC";
            } else {
                query += "\nORDER BY " + sortField + " DESC";
            }
        }


        query = query + "\nSKIP " + pageable.getOffset()
                + "\nLIMIT " + pageable.getPageSize();

        String countQuery = template.cypher_match_fragment.trim() + "\n" + template.cypher_count_fragment.trim();

        System.err.println("Running query: " + query + "\nWith parameters: " + paramMap + "\nCount query: " + countQuery);

        EagerResult res = getClient(subgraph).getDriver().executableQuery(query)
            .withParameters(paramMap)
            .withConfig(QueryConfig.builder().withDatabase("neo4j").build()).execute();

        EagerResult countRes = getClient(subgraph).getDriver().executableQuery(countQuery)
            .withParameters(paramMap)
            .withConfig(QueryConfig.builder().withDatabase("neo4j").build()).execute();
        
        if(countRes.records().isEmpty() || countRes.records().get(0).get("count") == null) {
            throw new RuntimeException("Count query did not return a count");
        }

        var count = countRes.records().get(0).get("count").asInt();
        if(count == 0) {
            return new org.springframework.data.domain.PageImpl<>(List.of(), pageable, 0);
        }


        List<QueryTemplate.ResultColumn> columns = template.result_columns;

        if(resolve) {

            var resolved = resolver.resolveToMap(
                subgraph,
                res.records().stream()
                    .flatMap(record -> columns.stream()
                        .filter(column -> column.column_type.equals("GraphNodeId"))
                        .map(column -> {
                            String columnId = column.column_id;
                            var value = record.get(columnId).asMap();
                            String nodeId = value.get("grebi:nodeId").toString();

                            // TODO ?? 
                            if(nodeId.startsWith(subgraph + ":")) {
                                nodeId = nodeId.substring(subgraph.length() + 1);
                            }

                            return nodeId;
                        })
                    )
                    .collect(Collectors.toSet())
            );

            var results =  res.records().stream().map(record -> {
                Map<String, Object> row = new HashMap<>();
                for (QueryTemplate.ResultColumn column : columns) {
                    String columnId = column.column_id;
                    if (column.column_type.equals("GraphNodeId")) {

                        var value = record.get(columnId).asMap();
                        String nodeId = value.get("grebi:nodeId").toString();

                        // TODO ??
                        if(nodeId.startsWith(subgraph + ":")) {
                            nodeId = nodeId.substring(subgraph.length() + 1);
                        }

                        row.put(columnId, resolved.get(nodeId));
                    } else {
                        row.put(columnId, record.get(columnId).asObject());
                    }
                }
                return row;
            }).collect(Collectors.toList());

            return new PageImpl<Map<String, Object>>(
                results,
                pageable,
                count
            );

        } else {
            return new PageImpl<Map<String, Object>>(
                res.records().stream()
                    .map(record -> {
                        Map<String, Object> row = new HashMap<>();

                        for (QueryTemplate.ResultColumn column : columns) {
                            String columnId = column.column_id;
                            if (column.column_type.equals("GraphNodeId")) {
                                var value = record.get(columnId).asMap();
                                String nodeId = value.get("grebi:nodeId").toString();

                                var valueCopy = new TreeMap<>(value);

                                // TODO ??
                                if(nodeId.startsWith(subgraph + ":")) {
                                    nodeId = nodeId.substring(subgraph.length() + 1);
                                }
                                valueCopy.put("grebi:nodeId", nodeId);

                                row.put(columnId, valueCopy);
                            } else {
                                row.put(columnId, record.get(columnId).asObject());
                            }
                        }

                        return row;
                    })
                    .collect(Collectors.toList()),
                pageable,
                count
            );
        }


    }



}
