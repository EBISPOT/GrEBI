package uk.ac.ebi.grebi.db;

        import java.util.List;
        import java.util.Map;
        import java.util.stream.Collectors;

        import com.google.common.base.Stopwatch;
        import com.google.gson.Gson;
        import com.google.gson.JsonElement;
        import com.google.gson.JsonParser;
        import org.neo4j.driver.*;
        import org.neo4j.driver.Record;
        import org.neo4j.driver.exceptions.NoSuchRecordException;
        import org.springframework.data.domain.*;

        import static org.neo4j.driver.Values.parameters;


public class Neo4jClient {

    static final String NEO4J_HOST = System.getenv("GREBI_NEO4J_HOST");

    public static String getNeo4jHost() {
        if(NEO4J_HOST != null)
            return NEO4J_HOST;
        return "bolt://localhost:7687/";
    }

    private Gson gson = new Gson();

    private Driver driver;

    public Driver getDriver() {

        if(driver == null) {
            driver = GraphDatabase.driver(getNeo4jHost());
        }

        return driver;

    }

    public Session getSession() {

        return getDriver().session(SessionConfig.forDatabase("neo4j"));

    }


    public List<Map<String,Object>> rawQuery(String query) {

        Session session = getSession();

        Result result = session.run(query);

        List<Map<String,Object>> list = result.stream().map(r -> r.asMap()).collect(Collectors.toList());

        session.close();
        return list;
    }

    public List<JsonElement> query(String query, String resVar) {

        Session session = getSession();

        Result result = session.run(query);


        List<JsonElement> list =  result.list().stream()
                .map(r -> r.get(resVar).get("_json").asString())
                .map(JsonParser::parseString)
                .collect(Collectors.toList());
        session.close();

        return list;
    }

    public Page<JsonElement> queryPaginated(String query, String resVar, String countQuery, Value parameters, Pageable pageable) {

        Session session = getSession();

        String sort = "";
        String queryToRun;

        if(pageable != null) {
            if(pageable.getSort() != null) {
                for (Sort.Order order : pageable.getSort()) {
                    if (sort.length() > 0) {
                        sort += ", ";
                    }
                    sort += order.getProperty();
                    sort += " ";
                    sort += order.getDirection() == Sort.Direction.ASC ? "ASC" : " DESC";
                }
            } else {
                sort = "ORDER BY " + resVar + ".iri ASC";
            }
            queryToRun = query + " " + sort + " SKIP " + pageable.getOffset() + " LIMIT " + pageable.getPageSize();
        } else {
            queryToRun = query;
        }

        System.out.println(queryToRun);
        System.out.println(gson.toJson(parameters.asMap()));

        Stopwatch timer = Stopwatch.createStarted();
        Result result = session.run(
                queryToRun,

                parameters
        );
        System.out.println("Neo4j run paginated query: " + timer.stop());

        Stopwatch timer2 = Stopwatch.createStarted();
        Result countResult = session.run(countQuery, parameters);
        System.out.println("Neo4j run paginated count: " + timer2.stop());

        Record countRecord = countResult.single();
        int count = countRecord.get(0).asInt();

        if(!result.hasNext() || result.peek().values().get(0).isNull()) {
            return new PageImpl<>(List.of(), pageable, count);
        }

        Page<JsonElement> page = new PageImpl<>(
                result.list().stream()
                        .map(r -> JsonParser.parseString(r.get(resVar).get("_json").asString()))
                        .collect(Collectors.toList()),
                pageable, count);

        session.close();
        return page;
    }

    public JsonElement queryOne(String query, String resVar, Value parameters) throws NoSuchRecordException {

        Session session = getSession();

        System.out.println(query);

        Stopwatch timer = Stopwatch.createStarted();
        Result result = session.run(query, parameters);
        System.out.println("Neo4j run query " + query + ": " + timer.stop());

        Value v = null;

        try {
            v = result.single().get(resVar).get("_json");
        } finally {
            session.close();
        }

        return JsonParser.parseString(v.asString());
    }

    public Page<JsonElement> getAll(String type, Map<String,String> properties, Pageable pageable) {

        String query = "MATCH (a:" + type + ")";

        if(properties.size() > 0) {
            query += " WHERE ";
            boolean isFirst = true;
            for(String property : properties.keySet()) {
                if(isFirst)
                    isFirst = false;
                else
                    query += " AND ";

                // TODO escape value
                query += "a." + property + " = \"" + properties.get(property) + "\"";
            }
        }

        String getQuery = query + " RETURN a";
        String countQuery = query + " RETURN count(a)";

        // TODO: can we just return _json ?
        // seems to break the neo4j client to return a string
        //
        return queryPaginated(getQuery, "a", countQuery, parameters("type", type), pageable);
    }

    public JsonElement getOne(String type, Map<String,String> properties) {

        Page<JsonElement> results = getAll(type, properties, PageRequest.of(0, 10));

        if(results.getTotalElements() != 1) {
            throw new RuntimeException("expected exactly one result for neo4j getOne, but got " + results.getTotalElements());
        }

        return results.getContent().iterator().next();
    }

    private String makeEdgePropsClause(Map<String,String> edgeProps) {

        String where = "";

        for(String prop : edgeProps.keySet()) {
            String value = edgeProps.get(prop);
            where += " AND \"" + value + "\" IN edge.`" + prop + "` ";
        }

        return where;
    }

    public Page<JsonElement> traverseOutgoingEdges(String type, String id, List<String> edgeIRIs, Map<String,String> edgeProps, Pageable pageable) {

        String edge = makeEdgesList(edgeIRIs, edgeProps);

        // TODO fix injection

        String query =
                "MATCH (a:" + type+ ")-[edge:" + edge + "]->(b) "
                        + "WHERE a.id = $id " + makeEdgePropsClause(edgeProps)
                        + "RETURN distinct b";

        String countQuery =
                "MATCH (a:" + type + ")-[edge:" + edge + "]->(b) "
                        + "WHERE a.id = $id " + makeEdgePropsClause(edgeProps)
                        + "RETURN count(distinct b)";

        System.out.println(query);

        return queryPaginated(query, "b", countQuery, parameters("type", type, "id", id), pageable);
    }

    public Page<JsonElement> traverseIncomingEdges(String type, String id, List<String> edgeIRIs, Map<String,String> edgeProps, Pageable pageable) {

        String edge = makeEdgesList(edgeIRIs, Map.of());

        String query =
                "MATCH (a:" + type + ")<-[edge:" + edge + "]-(b) "
                        + "WHERE a.id = $id "
                        + "RETURN distinct b";

        String countQuery =
                "MATCH (a:" + type + ")<-[edge:" + edge + "]-(b) "
                        + "WHERE a.id = $id "
                        + "RETURN count(distinct b)";

        return queryPaginated(query, "b", countQuery, parameters("type", type, "id", id), pageable);
    }

    public Page<JsonElement> recursivelyTraverseOutgoingEdges(String type, String id, List<String> edgeIRIs, Map<String,String> edgeProps, Pageable pageable) {

        String edge = makeEdgesList(edgeIRIs, Map.of());

        String query =
                "MATCH (c:" + type + ") WHERE c.id = $id "
                        + "WITH c "
                        + "OPTIONAL MATCH (c)-[edge:" + edge + " *]->(ancestor) "
                        + "RETURN DISTINCT ancestor AS a";

        String countQuery =
                "MATCH (a:" + type + ") WHERE a.id = $id "
                        + "WITH a "
                        + "OPTIONAL MATCH (a)-[edge:" + edge + " *]->(ancestor) "
                        + "RETURN count(DISTINCT ancestor)";

        return queryPaginated(query, "a", countQuery, parameters("type", type, "id", id), pageable);
    }

    public Page<JsonElement> recursivelyTraverseIncomingEdges(String type, String id, List<String> edgeIRIs, Map<String,String> edgeProps, Pageable pageable) {

        String edge = makeEdgesList(edgeIRIs, Map.of());

        String query =
                "MATCH (a:" + type + ") WHERE a.id = $id "
                        + "WITH a "
                        + "OPTIONAL MATCH (a)<-[edge:" + edge + " *]-(descendant) "
                        + "RETURN DISTINCT descendant AS c";

        String countQuery =
                "MATCH (a:" + type + ") WHERE a.id = $id "
                        + "WITH a "
                        + "OPTIONAL MATCH (a)<-[edge:" + edge + " *]-(descendant) "
                        + "RETURN count(DISTINCT descendant)";

        return queryPaginated(query, "c", countQuery, parameters("id", id), pageable);
    }

    private static String makeEdgesList(List<String> edgeIRIs, Map<String,String> edgeProperties) {

        String edge = "";

        for(String iri : edgeIRIs) {
            if(edge != "") {
                edge += "|";
            }

            edge += "`" + iri + "`";
        }

        return edge;
    }

}
