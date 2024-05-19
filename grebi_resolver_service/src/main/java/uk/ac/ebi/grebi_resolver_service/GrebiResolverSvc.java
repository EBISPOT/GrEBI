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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GrebiResolverSvc {
    private static RocksDB rocksDB;

    public static void main(String[] args) {


        Gson gson = new Gson();

        RocksDB.loadLibrary();

        Options options = new Options();
        options.setCreateIfMissing(false);

        try {
            rocksDB = RocksDB.openReadOnly(options, System.getenv("GREBI_ROCKSDB_PATH"));
        } catch (RocksDBException e) {
            e.printStackTrace();
            return;
        }

        Javalin app = Javalin.create(config -> {
        }).start(8080);

        app.post("/resolve", ctx -> {
            List<String> paramArray = gson.fromJson(new InputStreamReader(ctx.bodyInputStream()), List.class);
            List<byte[]> keys = new ArrayList<>();
            for (String id : paramArray) {
                keys.add(id.getBytes());
            }

            Map<String, JsonElement> results = new HashMap<>();
            try {
                List<byte[]> values = rocksDB.multiGetAsList(keys);
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
            if (rocksDB != null) {
                rocksDB.close();
            }
        }));
    }

}

