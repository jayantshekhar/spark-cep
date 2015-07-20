package com.cep.streaming.simple;

import com.cep.util.SparkConfUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by jayant on 5/4/15.
 */
public class StreamTextFiles {
    private StreamTextFiles() {
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: StreamTextFiles <in> <out>");
            System.exit(1);
        }

        // create streaming context
        Duration batchInterval = new Duration(20000);
        SparkConf sparkConf = new SparkConf().setAppName("StreamTextFiles");
        SparkConfUtil.setConf(sparkConf);
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);

        // create text file stream
        JavaDStream<String> textStream = ssc.textFileStream(args[0]);

        // count number of events in each RDD of the DStream
        textStream.count().map(new Function<Long, String>() {
            @Override
            public String call(Long in) {
                return "Received " + in + " events.";
            }
        }).print();

        // save the dstream
        textStream.dstream().saveAsTextFiles("aaa", "bbb");

        ssc.start();
        ssc.awaitTermination();

    }
}
