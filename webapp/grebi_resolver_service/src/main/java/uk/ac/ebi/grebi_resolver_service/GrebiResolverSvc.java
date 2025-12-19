package uk.ac.ebi.grebi_resolver_service;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import io.javalin.Javalin;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteOpenMode;

import java.io.InputStreamReader;
import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

public class GrebiResolverSvc {

    public static class Db {
        public Connection connection;
    }

    private static Map<String, Db> sqliteDBs = new HashMap<>();

    public static void main(String[] args) {

        Gson gson = new Gson();

        var dbfiles = Arrays.stream(new File(System.getenv("GREBI_SQLITE_SEARCH_PATH")).listFiles())
                .filter(File::isFile)
                .filter(f -> f.getName().endsWith(".sqlite") || f.getName().endsWith(".sqlite3"))
                .collect(Collectors.toList());

        System.out.println("Found sqlite files: " + dbfiles);

        for (var dbfile : dbfiles) {
            Db db = new Db();

            var subgraph = dbfile.getName().split("\\.")[0];

            System.out.println("Loading SQLite DB for subgraph " + subgraph + " from " + dbfile.getAbsolutePath());

            try {
                SQLiteConfig config = new SQLiteConfig();
                config.setReadOnly(true);
                config.setOpenMode(SQLiteOpenMode.READONLY);
                db.connection = DriverManager.getConnection("jdbc:sqlite:" + dbfile.getAbsolutePath(), config.toProperties());
            } catch (SQLException e) {
                e.printStackTrace();
                return;
            }

            sqliteDBs.put(subgraph, db);
            System.out.println("Loaded SQLite DB for subgraph " + subgraph + " from " + dbfile.getAbsolutePath());
        }

        int port = Integer.parseInt(System.getenv().getOrDefault("GREBI_RESOLVER_PORT", "8084"));
        Javalin app = Javalin.create(config -> {
        }).start("0.0.0.0", port);

        app.get("/health", ctx -> {
            ctx.contentType("application/json");
            ctx.result("{\"status\":\"ok\"}");
        });

        app.get("/subgraphs", ctx -> {
            ctx.contentType("application/json");
            ctx.result(gson.toJson(sqliteDBs.keySet()));
        });

        app.post("/{subgraph}/resolve", ctx -> {

            var subgraph = ctx.pathParam("subgraph");
            var sqliteDb = sqliteDBs.get(subgraph);
            if (sqliteDb == null) {
                ctx.status(404).result("Subgraph not found");
                return;
            }

            List<String> paramArray = gson.fromJson(new InputStreamReader(ctx.bodyInputStream()), List.class);
            Map<String, JsonElement> results = new HashMap<>();

            try (PreparedStatement stmt = sqliteDb.connection.prepareStatement(
                    "SELECT json FROM id_to_json WHERE id = ?")) {

                for (String id : paramArray) {
                    stmt.setBytes(1, id.getBytes());
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            var is = new InflaterInputStream(rs.getBinaryStream("json"));
                            JsonElement jsonElement = JsonParser.parseReader(new JsonReader(new InputStreamReader(is)));
                            results.put(id, jsonElement);
                        } else {
                            results.put(id, null);
                        }
                    }
                }

                ctx.contentType("application/json");
                ctx.result(gson.toJson(results));

            } catch (SQLException e) {
                ctx.status(500).result(e.getMessage());
            }

        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Db db : sqliteDBs.values()) {
                try {
                    if (db.connection != null && !db.connection.isClosed()) {
                        db.connection.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }));
    }
}
