package com.dataorc.spark.streaming;

import com.dataorc.spark.receiver.HttpReceiver;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class HttpStream {
    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setAppName("http receiver").setMaster("local[2]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));
        JavaReceiverInputDStream<String> lines = ssc.receiverStream(new HttpReceiver());
        lines.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                System.out.println(v1);
                return v1;
            }

        }) ;
        System.out.println("lines count is ");
        lines.count().print() ;
        lines.print();
        ssc.start();
        ssc.awaitTermination();
    }

    public static JavaInputDStream createInputDStream(JavaStreamingContext jsc){
        JavaInputDStream<String> lines = jsc.receiverStream(new HttpReceiver()) ;
        return lines;
    }
}
