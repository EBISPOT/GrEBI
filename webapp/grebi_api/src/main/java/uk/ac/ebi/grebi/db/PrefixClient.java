package uk.ac.ebi.grebi.db;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.ContentType;

public class PrefixClient {

    static final String PREFIX_HOST = System.getenv("GREBI_PREFIX_HOST");


    public static String getPrefixHost() {
        if (PREFIX_HOST != null)
            return PREFIX_HOST;
        return "http://localhost:8082/";
    }

    public List<String> reprefix(List<String> strs) {
        try {
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpPost request = new HttpPost(getPrefixHost() + "/reprefix");
            
            Map<String, List<String>> requestBody = Map.of("iris_or_curies", strs);
            String json = new Gson().toJson(requestBody);
            StringEntity entity = new StringEntity(json, ContentType.APPLICATION_JSON);
            request.setEntity(entity);
            
            HttpResponse response = httpClient.execute(request);
            HttpEntity responseEntity = response.getEntity();
            String responseString = EntityUtils.toString(responseEntity, "UTF-8");
            
            TypeToken<Map<String, List<String>>> typeToken = new TypeToken<Map<String, List<String>>>() {};
            Map<String, List<String>> responseMap = new Gson().fromJson(responseString, typeToken.getType());
            return responseMap.get("curies");
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to reprefix: " + strs, e);
        }
    }



}
