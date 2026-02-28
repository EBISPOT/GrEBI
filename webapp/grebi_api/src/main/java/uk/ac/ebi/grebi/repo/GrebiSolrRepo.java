package uk.ac.ebi.grebi.repo;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.springframework.data.domain.Pageable;

import com.google.gson.JsonElement;

import uk.ac.ebi.grebi.GrebiFacetedResultsPage;
import uk.ac.ebi.grebi.db.GrebiSolrClient;
import uk.ac.ebi.grebi.db.GrebiSolrQuery;
import uk.ac.ebi.grebi.db.ResolverClient;

public class GrebiSolrRepo {

    GrebiSolrClient solrClient = new GrebiSolrClient();
    ResolverClient resolver = new ResolverClient();
    Gson gson = new Gson();

    public GrebiSolrRepo() {
    }


    public Set<String> getSubgraphs() {

        var cores = solrClient.listCores();

        var autocompleteCores = cores.stream().filter(core -> core.startsWith("grebi_autocomplete_")).map(core -> core.replace("grebi_autocomplete_", "")).collect(Collectors.toSet());
        var nodesCores = cores.stream().filter(core -> core.startsWith("grebi_nodes_")).map(core -> core.replace("grebi_nodes_", "")).collect(Collectors.toSet());
        var edgesCores = cores.stream().filter(core -> core.startsWith("grebi_edges_")).map(core -> core.replace("grebi_edges_", "")).collect(Collectors.toSet());

        if (new HashSet<>(List.of(autocompleteCores, nodesCores, edgesCores)).size() != 1) {
            throw new RuntimeException("autocomplete, nodes, and edges cores must be present for all subgraphs. Found cores: " + String.join(",", cores));
        }

        return autocompleteCores; // any will do they are identical
    }

    public List<String> autocomplete(String subgraph, String q) {
        return solrClient.autocomplete(subgraph, q);
    }

    public GrebiFacetedResultsPage<Map<String,Object>> mapSolrFields(GrebiFacetedResultsPage<SolrDocument> solrDocs) {
        return solrDocs.map(doc -> {
            Map<String, Object> map = new LinkedTreeMap<>();
            for (String fieldName : doc.getFieldNames()) {
                if(!fieldName.startsWith("str_")) {
                    continue;
                }
                Object value = doc.getFieldValue(fieldName);
                fieldName = fieldName.replace("str_", "");
                fieldName = fieldName.replace("__", ":");
                if (value instanceof Collection) {
                    map.put(fieldName, new ArrayList<>((Collection<?>) value));
                } else {
                    map.put(fieldName, value);
                }
            }
            for (String fieldName : doc.getFieldNames()) {
                if(fieldName.startsWith("str_")) {
                    continue;
                }
                Object value = doc.getFieldValue(fieldName);
                fieldName = fieldName.replace("__", ":");
                if(map.containsKey(fieldName)) {
                    continue; // already set from str_ version
                }
                if (value instanceof Collection) {
                    map.put(fieldName, new ArrayList<>((Collection<?>) value));
                } else {
                    map.put(fieldName, value);
                }
            }

            return map;
        });
    }

    public GrebiFacetedResultsPage<Map<String, Object>> searchNodesPaginated(String subgraph, GrebiSolrQuery query, boolean resolve, Pageable pageable) {
        var res = solrClient.searchSolrPaginated("grebi_nodes_" + subgraph, query, pageable);
        return resolve ? resolveNodeIds(subgraph, res) : mapSolrFields(res);
    }

    public Map<String, Object> getFirstNode(String subgraph, GrebiSolrQuery query) {
        return resolveNodeId(subgraph, solrClient.getFirst("grebi_nodes_" + subgraph, query));
    }

    private GrebiFacetedResultsPage<Map<String, Object>> resolveNodeIds(String subgraph, GrebiFacetedResultsPage<SolrDocument> solrDocs) {

        List<String> ids = solrDocs.map(doc -> (String) doc.getFieldValues("str_grebi__nodeId").stream().iterator().next()).toList();

        List<Map<String, Object>> vals = resolver.resolveToList(subgraph, ids);
        assert (vals.size() == solrDocs.getSize());

        return new GrebiFacetedResultsPage<>(vals, solrDocs.facetFieldToCounts, solrDocs.getPageable(), solrDocs.getTotalElements());
    }

    private Map<String, Object> resolveNodeId(String subgraph, SolrDocument solrDoc) {
        return resolver.resolveToList(subgraph, List.of((String) solrDoc.getFieldValues("str_grebi__nodeId").stream().iterator().next())).iterator().next();
    }

    private GrebiFacetedResultsPage<Map<String, Object>> resolveEdgeIds(String subgraph, GrebiFacetedResultsPage<SolrDocument> solrDocs) {

        List<String> ids = solrDocs.map(doc -> (String) doc.getFieldValues("grebi__edgeId").stream().iterator().next()).toList();

        List<Map<String, Object>> vals = resolver.resolveToList(subgraph, ids);
        assert (vals.size() == solrDocs.getSize());

        return new GrebiFacetedResultsPage<>(vals, solrDocs.facetFieldToCounts, solrDocs.getPageable(), solrDocs.getTotalElements());
    }

    private Map<String, Object> resolveEdgeId(String subgraph, SolrDocument solrDoc) {

        return resolver.resolveToList(subgraph, List.of(solrDoc.getFieldValue("grebi__edgeId").toString())).iterator().next();
    }

    public GrebiFacetedResultsPage<Map<String, Object>> searchEdgesPaginated(String subgraph, GrebiSolrQuery query, Pageable pageable) {
        return resolveEdgeIds(subgraph, solrClient.searchSolrPaginated("grebi_edges_" + subgraph, query, pageable));
    }

    /**
     * Lightweight edge search that skips the full edge resolver.
     * Reads only indexed fields from Solr (type, datasources, fromNodeId, toNodeId)
     * then converts from/to node IDs to lightweight node refs.
     * Much faster than searchEdgesPaginated for use cases that only need edge refs.
     */
    public GrebiFacetedResultsPage<Map<String, Object>> searchEdgeRefsPaginated(String subgraph, GrebiSolrQuery query, Pageable pageable) {
        // Limit Solr to return only the fields we need for the lightweight response
        query.addReturnField("grebi:type");
        query.addReturnField("grebi:datasources");
        query.addReturnField("grebi:fromNodeId");
        query.addReturnField("grebi:toNodeId");

        var solrDocs = solrClient.searchSolrPaginated("grebi_edges_" + subgraph, query, pageable);
        var mapped = mapSolrFields(solrDocs);

        // Collect unique from/to node IDs
        Set<String> nodeIds = new LinkedHashSet<>();
        for (var edge : mapped.getContent()) {
            Object fromId = edge.get("grebi:fromNodeId");
            Object toId = edge.get("grebi:toNodeId");
            if (fromId instanceof String) nodeIds.add((String) fromId);
            if (toId instanceof String) nodeIds.add((String) toId);
        }

        // Convert node IDs to lightweight node refs (name, type, datasources, etc.)
        Map<String, Map<String, Object>> nodeRefs = nodeIdsToNodeRefs(subgraph, nodeIds);

        // Attach from/to node refs to each edge
        return mapped.map(edge -> {
            Map<String, Object> retEdge = new LinkedHashMap<>();
            retEdge.put("grebi:type", edge.get("grebi:type"));
            retEdge.put("grebi:datasources", edge.get("grebi:datasources"));
            retEdge.put("grebi:fromNodeId", edge.get("grebi:fromNodeId"));
            retEdge.put("grebi:toNodeId", edge.get("grebi:toNodeId"));
            Object fromId = edge.get("grebi:fromNodeId");
            Object toId = edge.get("grebi:toNodeId");
            if (fromId instanceof String && nodeRefs.containsKey(fromId)) {
                retEdge.put("from", nodeRefs.get(fromId));
            }
            if (toId instanceof String && nodeRefs.containsKey(toId)) {
                retEdge.put("to", nodeRefs.get(toId));
            }
            return retEdge;
        });
    }

    public Map<String, Map<String, Integer>> getIncomingEdgeCounts(String subgraph, String nodeId) {
        return getEdgeCounts(subgraph, "grebi__toNodeId", nodeId);
    }

    public Map<String, Map<String, Integer>> getOutgoingEdgeCounts(String subgraph, String nodeId) {
        return getEdgeCounts(subgraph, "grebi__fromNodeId", nodeId);
    }

    private Map<String, Map<String, Integer>> getEdgeCounts(String subgraph, String filterField, String nodeId) {
        SolrQuery q = new SolrQuery();
        q.setQuery("*:*");
        q.addFilterQuery(filterField + ":\"" + nodeId.replace("\"", "\\\"") + "\"");
        q.setRows(0); // we only need facets, no documents
        q.addFacetPivotField("grebi__type,grebi__datasources");
        q.setFacetLimit(-1); // return all facet values
        QueryResponse r = solrClient.runSolrQuery("grebi_edges_" + subgraph, q, null);
        return pivotsToMaps(r);
    }

    /**
     * Convert a collection of node IDs into lightweight node ref maps
     * containing only the fields needed by the UI (GraphNodeRef).
     *
     * TODO: Currently this fully resolves each node via the resolver service
     * and then strips down to ref fields. This is wasteful — once a reduced
     * Solr/SQLite index is available that stores only ref-level fields, this
     * method should query that directly instead of resolving the full node.
     */
    private Map<String, Map<String, Object>> nodeIdsToNodeRefs(String subgraph, Collection<String> nodeIds) {
        if (nodeIds.isEmpty()) return Collections.emptyMap();

        // Fully resolve nodes (temporary — returns all fields including embeddings, refs, etc.)
        Map<String, Map<String, Object>> fullNodes = resolver.resolveToMap(subgraph, nodeIds);
        if (fullNodes == null) return Collections.emptyMap();

        // Strip down to only the fields needed for a node ref
        Set<String> REF_FIELDS = Set.of(
                "grebi:nodeId", "grebi:name", "grebi:datasources",
                "grebi:type", "grebi:sourceIds", "ols:curie"
        );

        Map<String, Map<String, Object>> refs = new LinkedHashMap<>();
        for (var entry : fullNodes.entrySet()) {
            if (entry.getValue() == null) continue;
            Map<String, Object> ref = new LinkedHashMap<>();
            for (String field : REF_FIELDS) {
                if (entry.getValue().containsKey(field)) {
                    ref.put(field, entry.getValue().get(field));
                }
            }
            refs.put(entry.getKey(), ref);
        }
        return refs;
    }

    /**
     * Fetch both incoming and outgoing edge counts in a single method call.
     * The two Solr facet queries run in parallel to reduce latency.
     */
    public Map<String, Map<String, Map<String, Integer>>> getBothEdgeCounts(String subgraph, String nodeId) {
        CompletableFuture<Map<String, Map<String, Integer>>> inFuture =
                CompletableFuture.supplyAsync(() -> getEdgeCounts(subgraph, "grebi__toNodeId", nodeId));
        CompletableFuture<Map<String, Map<String, Integer>>> outFuture =
                CompletableFuture.supplyAsync(() -> getEdgeCounts(subgraph, "grebi__fromNodeId", nodeId));

        Map<String, Map<String, Map<String, Integer>>> result = new LinkedHashMap<>();
        result.put("incoming", inFuture.join());
        result.put("outgoing", outFuture.join());
        return result;
    }

    private Map<String, Map<String, Integer>> pivotsToMaps(QueryResponse r) {
        var pf = r.getFacetPivot().get("grebi__type,grebi__datasources");
        Map<String, Map<String, Integer>> res = new LinkedTreeMap<>();
        for (var f : pf) {
            String type = (String) f.getValue();
            for (var pivot : f.getPivot()) {
                String datasource = (String) pivot.getValue();
                int count = pivot.getCount();
                var dsToCount = res.get(type);
                if (dsToCount == null) {
                    dsToCount = new LinkedTreeMap<>();
                    res.put(type, dsToCount);
                }
                dsToCount.put(datasource, count);
            }
        }
        return res;
    }

    public GrebiFacetedResultsPage<Map<String, Object>> searchResultsPaginated(
            String subgraph, String queryid, GrebiSolrQuery q, Pageable pageable) {
        String core = "grebi_results__" + subgraph + "__" + queryid;
        if(!solrClient.listCores().contains(core))
            throw new RuntimeException("results core " + core + " not found");
        var page = solrClient.searchSolrPaginated(core, q, pageable);

        return page.map(row -> {
            var map = new HashMap<String,Object>();
            for(var k : row.keySet()) {
                var v = row.get(k);
                if(k.equals("_refs")) {
                    var refs_parsed = gson.fromJson((String)v, Map.class);
                    map.put("_refs", refs_parsed);
                } else {
                    map.put(k, v);
                }
            }
            return map;
        });
    }

    public static class SimilarResult {
        public Map<String, Object> node;
        public double score;
    }

    public List<SimilarResult> getSimilar(String subgraph, String nodeId, int n, String modelId) {
        // First, get the embedding vector for the source node
        SolrQuery getNodeQuery = new SolrQuery();
        getNodeQuery.setQuery("grebi__nodeId:" + nodeId);
        getNodeQuery.setFields("embedding__" + modelId);
        getNodeQuery.setRows(1);
        
        QueryResponse getNodeResponse = solrClient.runSolrQuery("grebi_nodes_" + subgraph, getNodeQuery, Pageable.ofSize(1));
        
        if (getNodeResponse.getResults().isEmpty()) {
            throw new RuntimeException("Node not found: " + nodeId);
        }
        
        SolrDocument sourceNode = getNodeResponse.getResults().get(0);
        Object embeddingObj = sourceNode.getFieldValue("embedding__" + modelId);
        
        if (embeddingObj == null) {
            throw new RuntimeException("No embedding found for node: " + nodeId + " with model: " + modelId);
        }
        
        // Convert embedding to the format needed for vector search
        List<Float> embeddingVector;
        if (embeddingObj instanceof List) {
            embeddingVector = new ArrayList<>();
            for (Object val : (List<?>) embeddingObj) {
                if (val instanceof Number) {
                    embeddingVector.add(((Number) val).floatValue());
                }
            }
        } else {
            throw new RuntimeException("Embedding is not in the expected format");
        }
        
        // Now perform KNN search
        SolrQuery knnQuery = new SolrQuery();
        knnQuery.setQuery("{!knn f=embedding__" + modelId + " topK=" + n + "}" + embeddingVectorToString(embeddingVector));
        knnQuery.setFields("grebi__nodeId", "str_grebi__name", "score");
        knnQuery.setRows(n);
        
        QueryResponse knnResponse = solrClient.runSolrQuery("grebi_nodes_" + subgraph, knnQuery, Pageable.ofSize(n));
        
        List<SimilarResult> results = new ArrayList<>();
        
        for (SolrDocument doc : knnResponse.getResults()) {
            SimilarResult result = new SimilarResult();
            result.score = (Double) doc.getFieldValue("score");
            
            // Resolve the full node information
            String similarNodeId = (String) doc.getFieldValue("grebi__nodeId");
            result.node = resolver.resolveToList(subgraph, List.of(similarNodeId)).get(0);
            
            results.add(result);
        }
        
        return results;
    }
    
    private String embeddingVectorToString(List<Float> vector) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < vector.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(vector.get(i));
        }
        sb.append("]");
        return sb.toString();
    }



}