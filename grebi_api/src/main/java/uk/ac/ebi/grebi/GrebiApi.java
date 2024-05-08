


package uk.ac.ebi.grebi;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.javalin.Javalin;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import io.javalin.plugin.bundled.CorsPluginConfig;
import org.neo4j.driver.*;
import org.springframework.data.domain.PageRequest;
import uk.ac.ebi.grebi.repo.GrebiNeoRepo;
import uk.ac.ebi.grebi.db.GrebiSolrClient;
import uk.ac.ebi.grebi.db.GrebiSolrQuery;
import uk.ac.ebi.grebi.repo.GrebiSolrRepo;


public class GrebiApi {



    public static void main(String[] args) throws ParseException, org.apache.commons.cli.ParseException, IOException {

        String solr_host = System.getenv("GREBI_SOLR_HOST");

        final GrebiNeoRepo neo = new GrebiNeoRepo();
        final GrebiSolrRepo solr = new GrebiSolrRepo();

//        Options options = new Options();
//
//        Option opt_metadata_json = new Option(null, "metadata_json", true, "path to metadata.json");
//        opt_metadata_json.setRequired(true);
//        options.addOption(opt_metadata_json);
//
//        CommandLineParser parser = new DefaultParser();
//        CommandLine cmd = parser.parse(options, args);
//
//        String metadata_json = cmd.getOptionValue("metadata_json");

        Gson gson = new Gson();
//        GraphMetadata md = gson.fromJson(new FileReader(metadata_json), GraphMetadata.class);

        var edgeTypes = neo.getEdgeTypes();
        var stats = neo.getStats();


        GrebiSolrClient solrClient = new GrebiSolrClient();


        var app = Javalin.create(config -> {
                    config.bundledPlugins.enableCors(cors -> {
                        cors.addRule(CorsPluginConfig.CorsRule::anyHost);
                    });
                })
                .get("/api/v1/stats", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(stats));
                })
                .get("/api/v1/nodes/{nodeId}", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result("{}");

                    var q = new GrebiSolrQuery();
                    q.addFilter("grebi:nodeId", List.of(ctx.pathParam("nodeId")), SearchType.WHOLE_FIELD);

                    var res = solrClient.getFirst(q);

                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
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
                    q.addSearchField("id", 500, SearchType.CASE_INSENSITIVE_TOKENS);
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
                    var res = solrClient.searchSolrPaginated(q, page);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .get("/api/v1/suggest", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result("{\"response\":{\"docs\":[]}}");
                })
                .start(8080);
    }

}

