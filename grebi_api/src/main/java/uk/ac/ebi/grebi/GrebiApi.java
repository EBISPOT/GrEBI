


package uk.ac.ebi.grebi;

import com.google.gson.Gson;
import io.javalin.Javalin;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.ParseException;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.EagerResult;
import org.neo4j.driver.GraphDatabase;
import org.apache.commons.cli.*;
import org.neo4j.driver.QueryConfig;


public class GrebiApi {
    public static void main(String[] args) throws ParseException, org.apache.commons.cli.ParseException, FileNotFoundException {

        Options options = new Options();

        Option opt_metadata_json = new Option(null, "metadata_json", true, "path to metadata.json");
        opt_metadata_json.setRequired(true);
        options.addOption(opt_metadata_json);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String metadata_json = cmd.getOptionValue("metadata_json");

        Gson gson = new Gson();
        GraphMetadata md = gson.fromJson(new FileReader(metadata_json), GraphMetadata.class);

        try (var driver = GraphDatabase.driver("neo4j://localhost")) {

            driver.verifyConnectivity();

            var app = Javalin.create(/*config*/)
                    .get("/", ctx -> {
                        ctx.contentType("application/json");
                        ctx.result(gson.toJson(md));
                    })
                    .post("/cypher", ctx -> {

                        EagerResult res = driver.executableQuery( ctx.body() )
                                .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
                                .execute();

                        ctx.contentType("application/json");
                        ctx.result(gson.toJson(res.records()));
                    })
                    .start(8080);

        }
    }
}