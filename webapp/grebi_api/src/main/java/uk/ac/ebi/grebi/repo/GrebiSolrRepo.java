package uk.ac.ebi.grebi.repo;

import java.util.*;
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
                if(fieldName.startsWith("str_")) {
                    continue;
                }
                Object value = doc.getFieldValue(fieldName);
                fieldName = fieldName.replace("__", ":");
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

        List<String> ids = solrDocs.map(doc -> doc.getFieldValue("grebi__nodeId").toString()).toList();

        List<Map<String, Object>> vals = resolver.resolveToList(subgraph, ids);
        assert (vals.size() == solrDocs.getSize());

        return new GrebiFacetedResultsPage<>(vals, solrDocs.facetFieldToCounts, solrDocs.getPageable(), solrDocs.getTotalElements());
    }

    private Map<String, Object> resolveNodeId(String subgraph, SolrDocument solrDoc) {
        return resolver.resolveToList(subgraph, List.of(solrDoc.getFieldValue("grebi__nodeId").toString())).iterator().next();
    }

    private GrebiFacetedResultsPage<Map<String, Object>> resolveEdgeIds(String subgraph, GrebiFacetedResultsPage<SolrDocument> solrDocs) {

        List<String> ids = solrDocs.map(doc -> doc.getFieldValue("grebi__edgeId").toString()).toList();

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

    public Map<String, Map<String, Integer>> getIncomingEdgeCounts(String subgraph, String nodeId) {
        SolrQuery q = new SolrQuery();
        q.set("defType", "edismax");
        q.set("qf", "grebi__toNodeId");
        q.setQuery(nodeId);
        q.addFacetPivotField("grebi__type,grebi__datasources");
        QueryResponse r = solrClient.runSolrQuery("grebi_edges_" + subgraph, q, Pageable.ofSize(1));
        return pivotsToMaps(r);
    }

    public Map<String, Map<String, Integer>> getOutgoingEdgeCounts(String subgraph, String nodeId) {
        SolrQuery q = new SolrQuery();
        q.set("defType", "edismax");
        q.set("qf", "grebi__fromNodeId");
        q.setQuery(nodeId);
        q.addFacetPivotField("grebi__type,grebi__datasources");
        QueryResponse r = solrClient.runSolrQuery("grebi_edges_" + subgraph, q, Pageable.ofSize(1));
        return pivotsToMaps(r);
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



}
