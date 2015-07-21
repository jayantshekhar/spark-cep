package com.cep.streaming.stocks;

import com.cep.streaming.dataset.Stock;
import com.cep.streaming.processor.ProcessStreamsAndEvaluateRules;
import com.cep.streaming.rule.StreamingRule;
import com.cep.util.SparkConfUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by jayant on 7/20/15.
 */
public class StocksCEP {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: StocksCEP <in> <out>");
            System.exit(1);
        }

        // create streaming context
        Duration batchInterval = new Duration(20000);
        SparkConf sparkConf = new SparkConf().setAppName("SQLOnStockStream");
        SparkConfUtil.setConf(sparkConf);
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);

        // create text file stream
        JavaDStream<String> textStream = ssc.textFileStream(args[0]);

        // query stock dstream
        StreamingRule rule = new StreamingRule();
        rule.eventClass = "Stock";
        rule.eventListenerClass = "StockEventListener";
        rule.sql = "select count(*) from stocks";
        rule.tableName = "stocks";

        StreamingRule[] rules = new StreamingRule[1];
        rules[0] = rule;

        try {
            ProcessStreamsAndEvaluateRules.process(ssc, textStream, "StockEvent", "stocks", rules);
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        ssc.start();
        ssc.awaitTermination();


    }
}
