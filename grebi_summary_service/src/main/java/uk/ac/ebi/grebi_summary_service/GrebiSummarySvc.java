package uk.ac.ebi.grebi_summary_service;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.javalin.Javalin;
import io.javalin.http.Context;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GrebiSummarySvc {
    private static Map<String,JsonElement> jsons = new HashMap<>();

    public static void main(String[] args) throws FileNotFoundException {

        Gson gson = new Gson();

        var files = Arrays.stream(new File(System.getenv("GREBI_SUMMARY_JSON_SEARCH_PATH")).listFiles()).filter(File::isFile).filter(f -> f.getName().endsWith("_summary.json")).toArray(File[]::new);

        for (File f : files) {
            var subgraph = f.getName().split("_summary.json")[0];
            jsons.put(subgraph, gson.fromJson(new InputStreamReader(new FileInputStream(f)), JsonElement.class));
            System.out.println("Loaded summary JSON for subgraph " + subgraph + " from " + f.getAbsolutePath());
        }

        Javalin app = Javalin.create(config -> {
        }).start(8081);

        app.get("/", ctx -> {
            ctx.contentType("application/json");
            ctx.result(gson.toJson(jsons));
        });
    }

}

