


package uk.ac.ebi.grebi;

import com.google.gson.Gson;
import io.javalin.Javalin;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

import io.javalin.plugin.bundled.CorsPluginConfig;
import org.springframework.data.domain.PageRequest;
import uk.ac.ebi.grebi.repo.GrebiNeoRepo;
import uk.ac.ebi.grebi.db.GrebiSolrQuery;
import uk.ac.ebi.grebi.repo.GrebiSolrRepo;


public class GrebiApi {

    public static void main(String[] args) throws ParseException, org.apache.commons.cli.ParseException, IOException {

        final GrebiNeoRepo neo = new GrebiNeoRepo();
        final GrebiSolrRepo solr = new GrebiSolrRepo();

        Gson gson = new Gson();

        var edgeTypes = neo.getEdgeTypes();
        var stats = neo.getStats();

        Javalin.create(config -> {
                    config.bundledPlugins.enableCors(cors -> {
                        cors.addRule(CorsPluginConfig.CorsRule::anyHost);
                    });
                    config.router.contextPath = System.getenv("GREBI_CONTEXT_PATH");
                })
                .get("/api/v1/stats", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(stats));
                })
                .get("/api/v1/nodes/{nodeId}", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result("{}");

                    var q = new GrebiSolrQuery();
                    q.addFilter("grebi__nodeId", List.of(ctx.pathParam("nodeId")), SearchType.WHOLE_FIELD);

                    var res = solr.getFirstNode(q);

                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .get("/api/v1/nodes/{nodeId}/incoming_edges", ctx -> {
                   ctx.contentType("application/json");
                   ctx.result(gson.toJson(neo.getIncomingEdges(ctx.pathParam("nodeId"))));
                })
                .get("/api/v1/edge_types", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(edgeTypes));
                })
                .get("/api/v1/collections", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result("{}");
                })
                .get("/api/v1/search", ctx -> {
                    var q = new GrebiSolrQuery();
                    q.setSearchText(ctx.queryParam("q"));
                    q.setExactMatch(false);
                    q.addSearchField("id", 1000, SearchType.WHOLE_FIELD);
                    q.addSearchField("grebi__synonym", 900, SearchType.WHOLE_FIELD);
                    q.addSearchField("id", 500, SearchType.CASE_INSENSITIVE_TOKENS);
                    q.addSearchField("grebi__synonym", 450, SearchType.CASE_INSENSITIVE_TOKENS);
                    q.addSearchField("grebi__description", 400, SearchType.WHOLE_FIELD);
                    q.addSearchField("grebi__description", 250, SearchType.CASE_INSENSITIVE_TOKENS);
                    q.addSearchField("_text_", 1, SearchType.CASE_INSENSITIVE_TOKENS);
                    for(var param : ctx.queryParamMap().entrySet()) {
                        if(param.getKey().equals("q") ||
                                param.getKey().equals("page") ||
                                param.getKey().equals("size") ||
                                param.getKey().equals("exactMatch") ||
                                param.getKey().equals("includeObsoleteEntries") ||
                                param.getKey().equals("lang") ||
                                    param.getKey().equals("facet")
                        ) {
                            continue;
                        }
                        q.addFilter(param.getKey(), param.getValue(), SearchType.WHOLE_FIELD);
                    }
                    var page_num = ctx.queryParam("page");
                    if(page_num == null) {
                        page_num = "0";
                    }
                    var size = ctx.queryParam("size");
                    if(size == null) {
                        size = "100";
                    }
                    var page = PageRequest.of(Integer.parseInt(page_num), Integer.parseInt(size));
                    var res = solr.searchNodesPaginated(q, page);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .get("/api/v1/suggest", ctx -> {
                    var res = solr.autocomplete(ctx.queryParam("q"));
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .start(8090);
    }

}

