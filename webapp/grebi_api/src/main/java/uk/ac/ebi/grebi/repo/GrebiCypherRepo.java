package uk.ac.ebi.grebi.repo;

import com.google.gson.Gson;

import jakarta.servlet.http.HttpServletResponse;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import uk.ac.ebi.grebi.GrebiApi;
import uk.ac.ebi.grebi.db.CypherServiceClient;
import uk.ac.ebi.grebi.db.PrefixClient;
import uk.ac.ebi.grebi.db.GrebiPostgresClient;
import uk.ac.ebi.grebi.repo.QueryTemplate;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class GrebiCypherRepo {

    CypherServiceClient cypherClient;
    Set<String> graphs;

    GrebiPostgresClient pgClient = new GrebiPostgresClient();
    Gson gson = new Gson();
    PrefixClient prefixClient = new PrefixClient();

    public GrebiCypherRepo() throws IOException {
        cypherClient = new CypherServiceClient(CypherServiceClient.getCypherServiceUrl());
        graphs = cypherClient.getGraphs();
    }

    public Set<String> getGraphs() {
        return graphs;
    }

    final String STATS_QUERY = new String(GrebiApi.class.getResourceAsStream("/cypher/stats.cypher").readAllBytes(), StandardCharsets.UTF_8);
    final String INCOMING_EDGES_QUERY = new String(GrebiApi.class.getResourceAsStream("/cypher/incoming_edges.cypher").readAllBytes(), StandardCharsets.UTF_8);

    @SuppressWarnings("unchecked")
    public Map<String, Map<String,Object>> getStats() {
        Map<String, Map<String,Object>> graphToStats = new HashMap<>();
        for(var graph : graphs) {
            try {
                var records = cypherClient.query(graph, STATS_QUERY, Map.of());
                if (!records.isEmpty()) {
                    graphToStats.put(graph, (Map<String, Object>) records.get(0).values().iterator().next());
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to get stats for graph " + graph, e);
            }
        }
        return graphToStats;
    }

    public class EdgeAndNode {
        public Map<String,Object> edge, node;
        public EdgeAndNode(Map<String,Object> edge, Map<String,Object> node) {
            this.edge = edge;
            this.node = node;
        }
    }

    public List<EdgeAndNode> getIncomingEdges(String graph, String nodeId, Pageable pageable) {
        List<Map<String, Object>> records;
        try {
            records = cypherClient.query(graph, INCOMING_EDGES_QUERY, Map.of(
                    "nodeId", graph + ":" + nodeId,
                    "offset", pageable.getOffset(),
                    "limit", pageable.getPageSize()
            ));
        } catch (IOException e) {
            throw new RuntimeException("Failed to get incoming edges", e);
        }

        var resolved = pgClient.resolveToMap(
                graph,
                records.stream().flatMap(record -> {
                    return List.of(
                            removeGraphPrefix((String) record.get("otherId"), graph),
                            removeGraphPrefix((String) record.get("edgeId"), graph)
                    ).stream();
                }).collect(Collectors.toSet()));

        return records.stream().map(record -> {
            var otherId = removeGraphPrefix((String) record.get("otherId"), graph);
            var edgeId = removeGraphPrefix((String) record.get("edgeId"), graph);
            return new EdgeAndNode(resolved.get(edgeId), resolved.get(otherId));
        }).collect(Collectors.toList());
    }

    public static class SimilarResult {
        public Object node;
        public double score;
    }

    public List<SimilarResult> getSimilar(String graph, String nodeId, int n) {

		String query = "MATCH (c:GraphNode {`grebi:nodeId`: $id}) "
		+ "CALL db.index.vector.queryNodes('embeddings', $n, c.`embedding:text-embedding-3-small`) "
		+ "YIELD node AS similar, score "
		+ "RETURN similar { .id, .`grebi:nodeId`, .`grebi:name`,`grebi:type`: labels(similar) } as node, score "
		+ "ORDER BY score DESC ";

		ArrayList<SimilarResult> res = new ArrayList<>();

        List<Map<String, Object>> records;
        try {
            records = cypherClient.query(graph, query, Map.of(
                "id", graph + ":" + nodeId,
                "n", n
            ));
        } catch (IOException e) {
            throw new RuntimeException("Failed to get similar nodes", e);
        }

		for(var rmap : records) {
            SimilarResult resRow = new SimilarResult();
			resRow.score = ((Number) rmap.get("score")).doubleValue();
			resRow.node = rmap.get("node");
			res.add(resRow);
		}

		return res;
    }

    public static class DirectionAndEdgeType {
        public String direction;
        public String edgeType;
    }

    /**
     * For each (direction, edgeType) pair where there is exactly one edge,
     * resolve the connected node. Uses a dynamically-constructed UNION ALL
     * Cypher query with literal relationship types for optimal planner performance.
     */
    public Map<String, Map<String, Object>> resolveSingleEdges(String graph, String nodeId, List<DirectionAndEdgeType> items) {
        if (items == null || items.isEmpty()) {
            return Collections.emptyMap();
        }

        String prefixedNodeId = graph + ":" + nodeId;

        // Build a UNION ALL query with one branch per item, using the Cypher DSL
        // for safe relationship type escaping. Each branch matches exactly one
        // relationship of the specified type and direction, returning the other node's ID.

        org.neo4j.cypherdsl.core.Node n = org.neo4j.cypherdsl.core.Cypher.node("GraphNode")
                .withProperties("grebi:nodeId", org.neo4j.cypherdsl.core.Cypher.parameter("nodeId"));
        org.neo4j.cypherdsl.core.Node other = org.neo4j.cypherdsl.core.Cypher.node("GraphNode").named("other");

        List<org.neo4j.cypherdsl.core.Statement> branches = new ArrayList<>();

        for (var item : items) {
            org.neo4j.cypherdsl.core.Statement branch;
            if ("incoming".equals(item.direction)) {
                branch = org.neo4j.cypherdsl.core.Cypher
                        .match(other.relationshipTo(n, item.edgeType))
                        .returning(
                                other.property("grebi:nodeId").as("otherId"),
                                other.property("grebi:name").as("otherName"),
                                org.neo4j.cypherdsl.core.Cypher.literalOf(item.direction).as("dir"),
                                org.neo4j.cypherdsl.core.Cypher.literalOf(item.edgeType).as("et")
                        )
                        .limit(1)
                        .build();
            } else {
                branch = org.neo4j.cypherdsl.core.Cypher
                        .match(n.relationshipTo(other, item.edgeType))
                        .returning(
                                other.property("grebi:nodeId").as("otherId"),
                                other.property("grebi:name").as("otherName"),
                                org.neo4j.cypherdsl.core.Cypher.literalOf(item.direction).as("dir"),
                                org.neo4j.cypherdsl.core.Cypher.literalOf(item.edgeType).as("et")
                        )
                        .limit(1)
                        .build();
            }
            branches.add(branch);
        }

        // Combine with UNION ALL
        org.neo4j.cypherdsl.core.Statement combined = org.neo4j.cypherdsl.core.Cypher.unionAll(
                branches.toArray(new org.neo4j.cypherdsl.core.Statement[0])
        );

        String cypher = combined.getCypher();

        List<Map<String, Object>> records;
        try {
            records = cypherClient.query(graph, cypher, Map.of("nodeId", prefixedNodeId));
        } catch (IOException e) {
            throw new RuntimeException("Failed to resolve single edges", e);
        }

        if (records.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Map<String, Object>> resultMap = new LinkedHashMap<>();
        for (var r : records) {
            String dir = (String) r.get("dir");
            String et = (String) r.get("et");
            String rawId = (String) r.get("otherId");
            String cleanId = removeGraphPrefix(rawId, graph);

            Map<String, Object> nodeData = new LinkedHashMap<>();
            nodeData.put("grebi:nodeId", cleanId);

            var otherName = r.get("otherName");
            if (otherName != null) {
                nodeData.put("grebi:name", otherName);
            }

            resultMap.put(dir + "::" + et, nodeData);
        }
        return resultMap;
    }

    private String removeGraphPrefix(String id, String graph) {
        if(!id.startsWith(graph + ":")) {
            throw new RuntimeException();
        }
        return id.substring(graph.length() + 1);
    }

    class PreparedQuery {
        public String query;
        public String countQuery;
        public Map<String, Object> params;
    }

    PreparedQuery prepareQuery(
        String graph, 
        QueryTemplate template,
        Map<String, List<String>> params,
        Sort sort
    ) {

        if(!template.graphs.contains(graph)) {
            throw new IllegalArgumentException("Query template " + template.id + " is not available for graph " + graph);
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
                nodeId = prefixClient.reprefix(List.of(nodeId)).get(0);
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

        PreparedQuery preparedQuery = new PreparedQuery();

        preparedQuery.query = template.cypher_match_fragment.trim()
                + "\n" + template.cypher_return_fragment.trim();
        preparedQuery.countQuery = template.cypher_match_fragment.trim() + "\n" + template.cypher_count_fragment.trim();
        preparedQuery.params = paramMap;

        if(sort != null && !sort.isUnsorted()) {
            var sorts = sort.stream().collect(Collectors.toList());
            if(sorts.size() != 1) {
                throw new IllegalArgumentException("Sorting by multiple columns is not supported");
            }
            var sortField = sorts.get(0).getProperty();
            if(!template.result_columns.stream().anyMatch(c -> c.column_id.equals(sortField))) {
                throw new IllegalArgumentException("Sort column " + sortField + " not found; valid columns are: " +
                    template.result_columns.stream().map(c -> c.column_id).collect(Collectors.joining(", ")));
            }
            if(sorts.get(0).isAscending()) {
                preparedQuery.query += "\nORDER BY " + sortField + " ASC";
            } else {
                preparedQuery.query += "\nORDER BY " + sortField + " DESC";
            }
        }

        return preparedQuery;
    }


    public Page<Map<String,Object>> runQueryFromTemplatePaginated(
        String graph, 
        QueryTemplate template,
        Map<String, List<String>> params,
        boolean resolve,
        Pageable pageable
        ) {

        var preparedQuery = prepareQuery(graph, template, params, pageable.getSort());
        var query = preparedQuery.query;
        var countQuery = preparedQuery.countQuery;
        var paramMap = preparedQuery.params;

        query = query + "\nSKIP " + pageable.getOffset()
                + "\nLIMIT " + pageable.getPageSize();

        System.err.println("Running query: " + query + "\nWith parameters: " + paramMap + "\nCount query: " + countQuery);

        List<Map<String, Object>> records;
        List<Map<String, Object>> countRecords;
        try {
            records = cypherClient.query(graph, query, paramMap);
            countRecords = cypherClient.query(graph, countQuery, paramMap);
        } catch (IOException e) {
            throw new RuntimeException("Failed to run query template", e);
        }
        
        if(countRecords.isEmpty() || countRecords.get(0).get("count") == null) {
            throw new RuntimeException("Count query did not return a count");
        }

        var count = ((Number) countRecords.get(0).get("count")).intValue();
        if(count == 0) {
            return new org.springframework.data.domain.PageImpl<>(List.of(), pageable, 0);
        }

        List<QueryTemplate.ResultColumn> columns = template.result_columns;

        if(resolve) {

            var resolved = pgClient.resolveToMap(
                graph,
                records.stream()
                    .flatMap(record -> columns.stream()
                        .filter(column -> column.column_type.equals("GraphNodeId"))
                        .map(column -> {
                            String columnId = column.column_id;
                            @SuppressWarnings("unchecked")
                            var value = (Map<String, Object>) record.get(columnId);
                            String nodeId = value.get("grebi:nodeId").toString();

                            // TODO ?? 
                            if(nodeId.startsWith(graph + ":")) {
                                nodeId = nodeId.substring(graph.length() + 1);
                            }

                            return nodeId;
                        })
                    )
                    .collect(Collectors.toSet())
            );

            var results =  records.stream().map(record -> {
                Map<String, Object> row = new HashMap<>();
                for (QueryTemplate.ResultColumn column : columns) {
                    String columnId = column.column_id;
                    if (column.column_type.equals("GraphNodeId")) {

                        @SuppressWarnings("unchecked")
                        var value = (Map<String, Object>) record.get(columnId);
                        String nodeId = value.get("grebi:nodeId").toString();

                        // TODO ??
                        if(nodeId.startsWith(graph + ":")) {
                            nodeId = nodeId.substring(graph.length() + 1);
                        }

                        row.put(columnId, resolved.get(nodeId));
                    } else {
                        row.put(columnId, record.get(columnId));
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
                records.stream()
                    .map(record -> {
                        Map<String, Object> row = new HashMap<>();

                        for (QueryTemplate.ResultColumn column : columns) {
                            String columnId = column.column_id;
                            if (column.column_type.equals("GraphNodeId")) {
                                @SuppressWarnings("unchecked")
                                var value = (Map<String, Object>) record.get(columnId);
                                String nodeId = value.get("grebi:nodeId").toString();

                                var valueCopy = new TreeMap<>(value);

                                // TODO ??
                                if(nodeId.startsWith(graph + ":")) {
                                    nodeId = nodeId.substring(graph.length() + 1);
                                }
                                valueCopy.put("grebi:nodeId", nodeId);

                                row.put(columnId, valueCopy);
                            } else {
                                row.put(columnId, record.get(columnId));
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


    @SuppressWarnings("unchecked")
    public CompletableFuture<Void> runQueryFromTemplateStreamed(
            String graph,
            QueryTemplate template,
            Map<String, List<String>> params,
            Sort sort,
            PrintWriter writer
    ) throws IOException {

        List<QueryTemplate.ResultColumn> columns = template.result_columns;

        var csvColumns = new ArrayList<String>();

        for (QueryTemplate.ResultColumn column : columns) {
            if (column.column_type.equals("EdgeId")) {
                continue;
            }
            String columnId = column.column_id;
            if (column.column_type.equals("GraphNodeId")) {
                csvColumns.add(columnId + "_id");
                csvColumns.add(columnId + "_label");
            } else {
                csvColumns.add(columnId);
            }
        }

        writer.write(String.join(",", csvColumns));
        writer.write("\n");

        var preparedQuery = prepareQuery(graph, template, params, sort);

        return CompletableFuture.runAsync(() -> {
            try {
                cypherClient.streamQuery(graph, preparedQuery.query, preparedQuery.params, record -> {

                    boolean first = true;

                    for (QueryTemplate.ResultColumn column : columns) {

                        if (column.column_type.equals("EdgeProps")) {
                            continue;
                        }

                        if(first) {
                            first = false;
                        } else {
                            writer.write(",");
                        }

                        String columnId = column.column_id;
                        if (column.column_type.equals("GraphNodeId")) {
                            var value = (Map<String, Object>) record.get(columnId);

                            var sourceIds = (List<String>) value.get("id");
                            var nodeId = pickFavouriteSourceId(sourceIds);

                            String nodeLabel;

                            var names = (List<String>) value.get("grebi:name");
                            if(names == null || names.isEmpty()) {
                                nodeLabel = nodeId;
                            } else {
                                nodeLabel = names.get(0).toString();
                            }

                            writer.write("\"" + nodeId.replace("\"", "\"\"") + "\",");
                            writer.write("\"" + nodeLabel.replace("\"", "\"\"") + "\"");

                        } else {
                            String raw = Objects.toString(record.get(columnId), "");
                            writer.write("\"" + raw.replace("\"", "\"\"") + "\"");
                        }
                    }

                    writer.write("\n");
                });
                writer.flush();
            } catch (Exception e) {
                writer.write("ERROR: " + e.getMessage() + "\n");
                writer.flush();
                throw new RuntimeException(e);
            }
        });
    }

    public CompletableFuture<Void> runQueryFromTemplateStreamed(
            String graph,
            QueryTemplate template,
            Map<String, List<String>> params,
            Sort sort,
            HttpServletResponse res
    ) throws IOException {

        res.setContentType("text/csv");
        res.setCharacterEncoding("UTF-8");
        res.setHeader("Content-Disposition", "attachment; filename=\"" + template.id + ".csv\"");
        res.setStatus(HttpServletResponse.SC_OK);

        PrintWriter writer = res.getWriter();

        return runQueryFromTemplateStreamed(graph, template, params, sort, writer);
    }



    // TODO: move to config
    //
    private static List<String> FAVOURITE_PREFIXES = List.of(
        "grebi:",
        "biolink:",
        "ro:",
        "hp:",
        "mp:",
        "mondo:",
        "oba:",
        "efo:",
        "doid:",
        "hgnc:",
        "mgi:",
        "uniprot:",
        "pmid:",
        "chebi:",
        "MTBLS",
        "MTBLC"
    );

    private String pickFavouriteSourceId(List<String> ids) {

        if(ids == null || ids.isEmpty()) {
            return null;
        }

        for(String prefix : FAVOURITE_PREFIXES) {
            for(String id : ids) {
                    if(id.startsWith(prefix)) {
                        return id;
                    }
                }
            }
        

        return ids.get(0);
    }




}
