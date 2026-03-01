package uk.ac.ebi.grebi.repo;

import java.util.*;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.springframework.data.domain.Pageable;

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

        if (cores == null || cores.isEmpty()) {
            return Collections.emptySet();
        }

        var autocompleteCores = cores.stream().filter(core -> core.startsWith("grebi_autocomplete_")).map(core -> core.replace("grebi_autocomplete_", "")).collect(Collectors.toSet());
        var nodesCores = cores.stream().filter(core -> core.startsWith("grebi_nodes_")).map(core -> core.replace("grebi_nodes_", "")).collect(Collectors.toSet());

        if (!autocompleteCores.equals(nodesCores)) {
            throw new RuntimeException("autocomplete and nodes cores must be present for all subgraphs. Found cores: " + String.join(",", cores));
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