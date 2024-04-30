


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
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.neo4j.driver.*;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;


public class GrebiApi {

    static Driver driver;


    public static void main(String[] args) throws ParseException, org.apache.commons.cli.ParseException, IOException {

        String solr_host = System.getenv("GREBI_SOLR_HOST");

        final String STATS_QUERY = new String(GrebiApi.class.getResourceAsStream("/cypher/stats.cypher").readAllBytes(), StandardCharsets.UTF_8);
        final String SEARCH_QUERY = new String(GrebiApi.class.getResourceAsStream("/cypher/search.cypher").readAllBytes(), StandardCharsets.UTF_8);
        final String PROPS_QUERY = new String(GrebiApi.class.getResourceAsStream("/cypher/props.cypher").readAllBytes(), StandardCharsets.UTF_8);

        driver = GraphDatabase.driver("neo4j://localhost");
        driver.verifyConnectivity();


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

        EagerResult props_res = driver.executableQuery(PROPS_QUERY)
                .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
                .execute();

        Map<String,JsonElement> edge_types = new TreeMap<>();

        for(var r : props_res.records().get(0).values()) {
            System.out.println(r.asString());
            JsonObject prop_def = gson.fromJson(r.asString(), JsonElement.class).getAsJsonObject();
            edge_types.put(prop_def.get("grebi:id").getAsString(), prop_def);
        }


        GrebiSolrClient solrClient = new GrebiSolrClient();


        var app = Javalin.create(config -> {
                    config.bundledPlugins.enableCors(cors -> {
                        cors.addRule(CorsPluginConfig.CorsRule::anyHost);
                    });
                })
                .get("/api/v1/stats", ctx -> {
                    EagerResult res = driver.executableQuery(STATS_QUERY)
                            .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
                            .execute();
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res.records().get(0).values().get(0).asMap()));
                })
                .post("/api/v1/cypher", ctx -> {

                    EagerResult res = driver.executableQuery(ctx.body())
                            .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
                            .execute();

                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res.records().get(0).values().stream().map(GrebiApi::mapValue).collect(Collectors.toList())));
                })
                .get("/api/v1/nodes", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result("{}");
                })
                .get("/api/v1/edge_types", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(edge_types));
                })
                .get("/api/v1/subgraphs", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result("{}");
                })
                .get("/api/v1/search", ctx -> {
                    var q = new GrebiSolrQuery();
                    q.setSearchText(ctx.queryParam("q"));
                    q.setExactMatch(false);
                    for(var param : ctx.queryParamMap().entrySet()) {
                        if(param.getKey().equals("page") ||
                                param.getKey().equals("size") ||
                                    param.getKey().equals("facet")
                        ) {
                            continue;
                        }
                        q.addFilter(param.getKey(), param.getValue(), SearchType.WHOLE_FIELD);
                    }
                    var page = PageRequest.of(Integer.getInteger(ctx.queryParam("page")), Integer.getInteger(ctx.queryParam("size")));
                    var res = solrClient.searchSolrPaginated(q, page);
                    ctx.contentType("application/json");
                    ctx.result(res.toString());
                })
                .get("/api/v1/suggest", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result("{\"response\":{\"docs\":[]}}");
                })
                .start(8080);
    }

    static Map<String, Object> mapValue(Value value) {
        Map<String, Object> res = new TreeMap<>(value.asMap());
        res.put("grebi:type", StreamSupport.stream(value.asNode().labels().spliterator(), false).collect(Collectors.toList()));
        return res;
    }
}

