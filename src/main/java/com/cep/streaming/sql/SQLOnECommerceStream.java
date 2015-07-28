package com.cep.streaming.sql;

import com.cep.streaming.dataset.Click;
import com.cep.streaming.dataset.Person;
import com.cep.streaming.dataset.Transaction;
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
public class SQLOnECommerceStream {
    public  SQLOnECommerceStream() {
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: SQLOnECommerceStream <in_t> <in_c> <out>");
            System.exit(1);
        }

        // create streaming context
        Duration batchInterval = new Duration(20000);
        SparkConf sparkConf = new SparkConf().setAppName("SQLOnECommerceStream");
        SparkConfUtil.setConf(sparkConf);
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);

        // create text file stream
        JavaDStream<String> textStream = ssc.textFileStream(args[0]);

        // create the Person DStream
        JavaDStream<Transaction> transactionStream = textStream.map(new Function<String, Transaction>() {
            @Override
            public Transaction call(String str) {
                Transaction p = new Transaction(str);
                return p;
            }
        });

        // create text file stream
        JavaDStream<String> textStream1 = ssc.textFileStream(args[1]);

        // create the Person DStream
        JavaDStream<Click> clickStream = textStream1.map(new Function<String, Click>() {
            @Override
            public Click call(String str) {
                Click p = new Click(str);
                return p;
            }
        });

        // join the streams
        JavaDStream<Transaction> jdstream = joinStreams(ssc, transactionStream, clickStream);

        // query the joined stream
        queryJoinedStream(ssc, jdstream);

        ssc.start();
        ssc.awaitTermination();
    }

    // join the streams
    private static JavaDStream<Transaction> joinStreams(JavaStreamingContext ssc, JavaDStream<Transaction> transactionStream,
                                                   JavaDStream<Click> clickStream) {
        JavaPairDStream<Long, Transaction> tpair = transactionStream.mapToPair(new PairFunction<Transaction, Long, Transaction>() {
            @Override
            public Tuple2<Long, Transaction> call(Transaction p) {
                return new Tuple2<Long, Transaction>(p.getCustomerId(), p);
            }
        });

        JavaPairDStream<Long, Click> cpair = clickStream.mapToPair(new PairFunction<Click, Long, Click>() {
            @Override
            public Tuple2<Long, Click> call(Click p) {
                return new Tuple2<Long, Click>(p.getCustomerId(), p);
            }
        });

        // join the incoming streams
        JavaPairDStream<Long, Tuple2<Transaction, Click>> joinStream = tpair.join(cpair);

        joinStream.print();

        // map it to a joined dataset
        JavaDStream<Transaction> jdstream = joinStream.map(new Function<Tuple2<Long, Tuple2<Transaction, Click>>, Transaction>() {
            @Override
            public Transaction call(Tuple2<Long, Tuple2<Transaction, Click>> t2) {
                Transaction p = t2._2()._1();
                return p;
            }
        });

        return jdstream;
    }

    // query the joined stream
    private static void queryJoinedStream(JavaStreamingContext ssc, JavaDStream<Transaction> joinedStream) {

        // Convert RDDs of the words DStream to DataFrame and run SQL query
        joinedStream.foreachRDD(new Function2<JavaRDD<Transaction>, Time, Void>() {
            @Override
            public Void call(JavaRDD<Transaction> rdd, Time time) {
                SQLContext sqlContext = SQLContextSingleton.getInstance(rdd.context());

                // Convert JavaRDD[Person] to DataFrame
                DataFrame personDataFrame = sqlContext.createDataFrame(rdd, Transaction.class);

                // Register as table
                personDataFrame.registerTempTable("transactions");

                long count = personDataFrame.count();
                if (count > 0) {
                    System.out.println(""+count);
                }

                // Do word count on table using SQL and print it
                DataFrame personsCountsDataFrame =
                        sqlContext.sql("select count(*) as total from transactions");
                System.out.println("========= " + time + "=========");
                personsCountsDataFrame.show();

                return null;
            }
        });

    }

}


