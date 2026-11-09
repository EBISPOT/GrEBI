package uk.ac.ebi.grebi;

import org.springframework.data.domain.Pageable;
import uk.ac.ebi.grebi.db.GrebiPostgresClient;
import uk.ac.ebi.grebi.repo.GrebiPostgresRepo;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The full nodes a request's hits resolve to, in the language asked for:
 * whole, or, when the request gives maxValues, abridged (see
 * {@link Abridgement}). Abridged nodes are read from their blobs as streams,
 * so that a hub node among the hits, 100 MB and more whole, is never held
 * whole; this is what the node route does for one node.
 */
final class ResolvedNodes {

    private ResolvedNodes() {
    }

    /**
     * The nodes with the given ids, by id. With maxValues null they are
     * whole; an id with no node is not in the map.
     */
    static Map<String, Map<String, Object>> byId(
        GrebiPostgresClient pg, String graph, Collection<String> ids, Integer maxValues, String lang, boolean propertyRefs
    ) {
        var nodes = new LinkedHashMap<String, Map<String, Object>>();
        if (ids.isEmpty()) {
            return nodes;
        }
        if (maxValues == null) {
            for (var resolved : pg.resolveToMap(graph, ids).entrySet()) {
                nodes.put(resolved.getKey(), Languages.localise(resolved.getValue(), lang));
            }
        } else {
            for (var blob : pg.fetchBlobs(graph, ids).entrySet()) {
                nodes.put(blob.getKey(), Abridgement.read(blob.getValue(), maxValues, lang, propertyRefs));
            }
        }
        return nodes;
    }

    /**
     * A page of a node search: the hits as they are, or resolved to their
     * nodes, whole (maxValues null) or abridged.
     */
    static GrebiFacetedResultsPage<Map<String, Object>> searchPage(
        GrebiPostgresRepo postgres, String graph, String q, Map<String, List<String>> filters, boolean exactMatch,
        boolean resolve, Integer maxValues, String lang, boolean propertyRefs, Pageable page
    ) {
        if (!resolve) {
            return postgres.searchNodesPaginated(graph, q, filters, exactMatch, false, page);
        }
        if (maxValues == null) {
            return postgres.searchNodesPaginated(graph, q, filters, exactMatch, true, page).map(node -> Languages.localise(node, lang));
        }
        var hits = postgres.searchNodesPaginated(graph, q, filters, exactMatch, false, page);
        var ids = hits.getContent().stream().map(hit -> String.valueOf(hit.get("grebi:nodeId"))).toList();
        var blobs = ids.isEmpty() ? Map.<String, byte[]>of() : postgres.getPgClient().fetchBlobs(graph, ids);
        return new GrebiFacetedResultsPage<>(
            Abridgement.readHits(hits.getContent(), blobs, maxValues, lang, propertyRefs),
            hits.facetFieldToCounts, hits.getPageable(), hits.getTotalElements());
    }
}
