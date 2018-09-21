package com.dataorc.spark.receiver;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.net.URISyntaxException;

public class SimpleReceiver {

    static String httpBaseAddress = "http://localhost:5000/getvalues";
    static int defaultLimit = 5;
    static URIBuilder uribuilder = new URIBuilder();
    static java.net.URI uri = null ;

    public static void main(String[] args) throws IOException, URISyntaxException {

        java.net.URI uri = uribuilder.setScheme("http").setHost("localhost:5000").setPath("/getvalues").addParameter("numvals",String.valueOf(defaultLimit)).build() ;
        Content r = Request.Post(uri).
                execute().returnContent();
        JsonParser jsonParser = new JsonParser() ;
        JsonElement jsonElement = jsonParser.parse(r.asString());
        JsonArray jsonarray = jsonElement.getAsJsonArray() ;
        System.out.println(jsonarray);
    }

    public static void initialize(){
        try {
            uribuilder = new URIBuilder();
            uri = uribuilder.setScheme("http").setHost("localhost:5000").setPath("/getvalues").addParameter("numvals",String.valueOf(defaultLimit)).build() ;
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static JsonArray getvalues(){
        Content r = null;
        try {
            r = Request.Post(uri).
                    execute().returnContent();
            JsonParser jsonParser = new JsonParser() ;
            JsonElement jsonElement = jsonParser.parse(r.asString());
            JsonArray jsonarray = jsonElement.getAsJsonArray() ;
            return jsonarray ;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new JsonArray();
    }

    public static String check4py4j(){
        return "wassabi" ;
    }
}
