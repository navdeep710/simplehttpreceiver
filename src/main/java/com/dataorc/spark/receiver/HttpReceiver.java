package com.dataorc.spark.receiver;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class HttpReceiver extends Receiver<String>
{
    String httpBaseAddress ;
    int defaultLimit ;

    public HttpReceiver(StorageLevel storageLevel) {
        super(StorageLevel.MEMORY_AND_DISK_2());
    }

    public void setHttpReceiver(String baseAddress,int defaultLimit){
        this.httpBaseAddress = baseAddress ;
        this.defaultLimit = defaultLimit ;

    }

    public void onStart() {
        new Thread(this::receive).start();
    }

    public void onStop() {

    }

    private void receive()  {
        JsonArray array = SimpleReceiver.getvalues() ;
        for(JsonElement je: array){
            store(je.getAsString());
        }
    }
}
