package com.cep.streaming.sql;

import com.cep.streaming.dataset.Person;
import com.cep.streaming.dataset.Stock;
import com.cep.streaming.sql.SQLContextSingleton;
import com.cep.util.SparkConfUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * Created by jayant on 5/4/15.
 */
public class SQLOnStockStream {

    public  SQLOnStockStream() {
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: SQLOnStockStream <in> <out>");
            System.exit(1);
        }

        // create streaming context
        Duration batchInterval = new Duration(20000);
        SparkConf sparkConf = new SparkConf().setAppName("SQLOnStockStream");
        SparkConfUtil.setConf(sparkConf);
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);

        // create text file stream
        JavaDStream<String> textStream = ssc.textFileStream(args[0]);

        // create the Stock DStream
        JavaDStream<Stock> stockStream = textStream.map(new Function<String, Stock>() {
            @Override
            public Stock call(String str) {
                Stock p = new Stock(str);
                return p;
            }
        });

        // query stock dstream
        queryStockStream(ssc, stockStream);

        ssc.start();
        ssc.awaitTermination();
    }

    // query Stock DStream
    private static void queryStockStream(JavaStreamingContext ssc, JavaDStream<Stock> personStream) {

        // Convert RDDs of the words DStream to DataFrame and run SQL query
        personStream.foreachRDD(new Function2<JavaRDD<Stock>, Time, Void>() {
            @Override
            public Void call(JavaRDD<Stock> rdd, Time time) {
                SQLContext sqlContext = SQLContextSingleton.getInstance(rdd.context());

                // Convert JavaRDD[Stock] to DataFrame
                DataFrame stockDataFrame = sqlContext.createDataFrame(rdd, Stock.class);

                // Register as table
                stockDataFrame.registerTempTable("stocks");

                // Do word count on table using SQL and print it
                DataFrame stocksCountsDataFrame =
                        sqlContext.sql("select name, count(*) as total from stocks group by symbol");
                System.out.println("========= " + time + "=========");
                stocksCountsDataFrame.show();

                return null;
            }
        });
    }

}


