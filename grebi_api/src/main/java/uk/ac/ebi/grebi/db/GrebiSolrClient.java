package uk.ac.ebi.grebi.db;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Pageable;
import uk.ac.ebi.grebi.GrebiFacetedResultsPage;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GrebiSolrClient {

    private Gson gson = new Gson();

    private static final Logger logger = LoggerFactory.getLogger(GrebiSolrClient.class);
    public static final int MAX_ROWS = 1000;
    static final String SOLR_HOST = System.getenv("GREBI_SOLR_HOST");

    public static String getSolrHost() {
        if (SOLR_HOST != null)
            return SOLR_HOST;
        return "http://localhost:8983/";
    }

    public GrebiFacetedResultsPage<SolrDocument> searchSolrPaginated(GrebiSolrQuery query, Pageable pageable) {

        QueryResponse qr = runSolrQuery(query, pageable);

        Map<String, Map<String, Long>> facetFieldToCounts = new LinkedHashMap<>();

        if (qr.getFacetFields() != null) {
            for (FacetField facetField : qr.getFacetFields()) {

                Map<String, Long> valueToCount = new LinkedHashMap<>();

                for (FacetField.Count count : facetField.getValues()) {
                    valueToCount.put(count.getName(), count.getCount());
                }

                facetFieldToCounts.put(facetField.getName(), valueToCount);
            }
        }

        return new GrebiFacetedResultsPage<>(
                qr.getResults()
                        .stream()
                        .collect(Collectors.toList()),
                facetFieldToCounts,
                pageable,
                qr.getResults().getNumFound());
    }

    public SolrDocument getFirst(GrebiSolrQuery query) {

        QueryResponse qr = runSolrQuery(query, null);

        if (qr.getResults().getNumFound() < 1) {
            logger.info("Expected at least 1 result for solr getFirst for solr query = {}", query.constructQuery().jsonStr());
            throw new RuntimeException("Expected at least 1 result for solr getFirst");
        }

        return qr.getResults().get(0);
    }

    public QueryResponse runSolrQuery(GrebiSolrQuery query, Pageable pageable) {
        return runSolrQuery(query.constructQuery(), pageable);
    }

    public QueryResponse runSolrQuery(SolrQuery query, Pageable pageable) {

        if (pageable != null) {
            query.setStart((int) pageable.getOffset());
            query.setRows(pageable.getPageSize() > MAX_ROWS ? MAX_ROWS : pageable.getPageSize());
        }

        logger.info("solr rows: {} ", query.getRows());
        logger.info("solr query: {} ", query.toQueryString());
        logger.info("solr query urldecoded: {}", URLDecoder.decode(query.toQueryString()));
        logger.info("solr host: {}", SOLR_HOST);

        org.apache.solr.client.solrj.SolrClient mySolrClient = new HttpSolrClient.Builder(getSolrHost() + "/solr/grebi_nodes").build();

        QueryResponse qr = null;
        try {
            qr = mySolrClient.query(query);
            logger.info("solr query had {} result(s).", qr.getResults().getNumFound());
        } catch (SolrServerException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                mySolrClient.close();
            } catch (IOException ioe) {
                logger.error("Failed to close Solr client with exception \"{}\"", ioe.getMessage());
            }
        }
        return qr;
    }

    public List<String> autocomplete(String q) {
        org.apache.solr.client.solrj.SolrClient mySolrClient = new HttpSolrClient.Builder(getSolrHost() + "/solr/grebi_autocomplete").build();

        SolrQuery query = new SolrQuery();
        query.set("defType", "edismax");
        query.setFields("label");
        query.setQuery(q);
        query.set("qf", "label^10 edge_label^2 whitespace_edge_label^1");
        query.set("q.op", "AND");

        QueryResponse qr = null;
        try {
            qr = mySolrClient.query(query);
            logger.info("solr query had {} result(s).", qr.getResults().getNumFound());
        } catch (SolrServerException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                mySolrClient.close();
            } catch (IOException ioe) {
                logger.error("Failed to close Solr client with exception \"{}\"", ioe.getMessage());
            }
        }
        return qr.getResults().stream().map(r -> r.get("label").toString()).collect(Collectors.toList());
    }
}
