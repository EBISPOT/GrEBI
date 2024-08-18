package uk.ac.ebi.grebi.repo;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

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

    public GrebiSolrRepo() {}

    public Set<String> getSubgraphs() {

        var cores = solrClient.listCores();

        var autocompleteCores = cores.stream().filter(core -> core.startsWith("grebi_autocomplete_")).map(core -> core.replace("grebi_autocomplete_", "")).collect(Collectors.toSet());
        var nodesCores = cores.stream().filter(core -> core.startsWith("grebi_nodes_")).map(core -> core.replace("grebi_nodes_", "")).collect(Collectors.toSet());
        var edgesCores = cores.stream().filter(core -> core.startsWith("grebi_edges_")).map(core -> core.replace("grebi_edges_", "")).collect(Collectors.toSet());

        if(new HashSet<>(List.of(autocompleteCores, nodesCores, edgesCores)).size() != 1) {
            throw new RuntimeException("autocomplete, nodes, and edges cores must be present for all subgraphs. Found cores: " + String.join(",", cores));
        }

        return autocompleteCores; // any will do they are identical
    }

    public List<String> autocomplete(String subgraph, String q) {
        return solrClient.autocomplete(subgraph, q);
    }

    public GrebiFacetedResultsPage<Map<String,JsonElement>> searchNodesPaginated(String subgraph, GrebiSolrQuery query, Pageable pageable) {
        return resolveNodeIds(subgraph, solrClient.searchSolrPaginated("grebi_nodes_"+subgraph, query, pageable));
    }

    public Map<String,JsonElement> getFirstNode(String subgraph, GrebiSolrQuery query) {
        return resolveNodeId(subgraph, solrClient.getFirst("grebi_nodes_"+subgraph, query));
    }

    private GrebiFacetedResultsPage<Map<String,JsonElement>> resolveNodeIds(String subgraph, GrebiFacetedResultsPage<SolrDocument> solrDocs) {

        List<String> ids = solrDocs.map(doc -> doc.getFieldValue("grebi__nodeId").toString()).toList();

        List<Map<String,JsonElement>> vals = resolver.resolveToList(subgraph, ids);
        assert(vals.size() == solrDocs.getSize());

        return new GrebiFacetedResultsPage<>(vals, solrDocs.facetFieldToCounts, solrDocs.getPageable(), solrDocs.getTotalElements());
    }

    private Map<String,JsonElement> resolveNodeId(String subgraph, SolrDocument solrDoc)  {
        return resolver.resolveToList(subgraph, List.of(solrDoc.getFieldValue("grebi__nodeId").toString())).iterator().next();
    }

    private GrebiFacetedResultsPage<Map<String,JsonElement>> resolveEdgeIds(String subgraph, GrebiFacetedResultsPage<SolrDocument> solrDocs) {

        List<String> ids = solrDocs.map(doc -> doc.getFieldValue("grebi__edgeId").toString()).toList();

        List<Map<String,JsonElement>> vals = resolver.resolveToList(subgraph, ids);
        assert(vals.size() == solrDocs.getSize());

        return new GrebiFacetedResultsPage<>(vals, solrDocs.facetFieldToCounts, solrDocs.getPageable(), solrDocs.getTotalElements());
    }

    private Map<String,JsonElement> resolveEdgeId(String subgraph, SolrDocument solrDoc)  {

        return resolver.resolveToList(subgraph, List.of(solrDoc.getFieldValue("grebi__edgeId").toString())).iterator().next();
    }

}
