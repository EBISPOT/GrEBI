package uk.ac.ebi.grebi_resolver_service;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.InputStreamReader;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GrebiResolverSvc {
    private static Map<String,RocksDB> rocksDBs = new HashMap<>();

    public static void main(String[] args) {

        Gson gson = new Gson();

        RocksDB.loadLibrary();

        Options options = new Options();
        options.setCreateIfMissing(false);

        var dirs = Arrays.stream(new File(System.getenv("GREBI_ROCKSDB_SEARCH_PATH")).listFiles()).filter(File::isDirectory).filter(f -> f.getName().endsWith("_rocksdb")).toArray(File[]::new);

        for (File dir : dirs) {
            RocksDB rocksDB = null;
            try {
                rocksDB = RocksDB.openReadOnly(options, dir.getAbsolutePath());
            } catch (RocksDBException e) {
                e.printStackTrace();
                return;
            }
            var subgraph = dir.getName().split("_rocksdb")[0];
            rocksDBs.put(subgraph, rocksDB);
            System.out.println("Loaded RocksDB for subgraph " + subgraph + " from " + dir.getAbsolutePath());
        }

        Javalin app = Javalin.create(config -> {
        }).start(8080);

        app.get("/subgraphs", ctx -> {
            ctx.contentType("application/json");
            ctx.result(gson.toJson(rocksDBs.keySet()));
        });

        app.post("/{subgraph}/resolve", ctx -> {

            var subgraph = ctx.pathParam("subgraph");
            var rocksdb = rocksDBs.get(subgraph);
            if(rocksdb == null) {
                ctx.status(404).result("Subgraph not found");
                return;
            }

            List<String> paramArray = gson.fromJson(new InputStreamReader(ctx.bodyInputStream()), List.class);
            List<byte[]> keys = new ArrayList<>();
            for (String id : paramArray) {
                keys.add(id.getBytes());
            }

            Map<String, JsonElement> results = new HashMap<>();
            try {
                List<byte[]> values = rocksdb.multiGetAsList(keys);
                int n = 0;
                for (byte[] value : values) {
                    byte[] key = keys.get(n++);
                    if (value != null) {
                        JsonElement jsonElement = JsonParser.parseString(new String(value));
                        results.put(new String(key), jsonElement);
                    } else {
                        results.put(new String(key), null);
                    }
                }
                ctx.contentType("application/json");
                ctx.result(gson.toJson(results));
            } catch (RocksDBException e) {
                ctx.status(500).result(e.getMessage());
            }

        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (RocksDB rocksDB : rocksDBs.values()) {
                rocksDB.close();
            }
        }));
    }

}

