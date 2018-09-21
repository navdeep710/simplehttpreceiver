package com.dataorc.spark.receiver;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.IOException;
import java.net.URISyntaxException;

public class HttpReceiver extends Receiver<String> {
    String httpBaseAddress;
    int defaultLimit = 5;

    public HttpReceiver() {
        super(StorageLevel.MEMORY_AND_DISK_2());
    }

    public void setHttpReceiver(String baseAddress, int defaultLimit) {
        this.httpBaseAddress = baseAddress;
        this.defaultLimit = defaultLimit;

    }

    public void onStart() {
        new Thread(this::receive).start();
    }

    public void onStop() {

    }

    private void receive() {
        URIBuilder uribuilder = new URIBuilder();
        try {
            java.net.URI uri = uribuilder.setScheme("http").setHost("localhost:5000").setPath("/getvalues").addParameter("numvals", String.valueOf(defaultLimit)).build();
            while (!isStopped()) {
                Content r = Request.Post(uri).
                        execute().returnContent();
                JsonParser jsonParser = new JsonParser();
                JsonElement jsonElement = jsonParser.parse(r.asString());
                JsonArray jsonarray = jsonElement.getAsJsonArray();
                if (jsonarray.size() > 0) {
                    for (JsonElement je : jsonarray) {
                        store(je.getAsString());
                    }
                }
            }
        } catch (URISyntaxException | IOException e) {
            e.printStackTrace();
            restart("caught an exceptio restarting" + e.getMessage());

        }
    }
}
