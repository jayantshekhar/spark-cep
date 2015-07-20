package com.cep.streaming.sql;

import com.cep.streaming.dataset.Person;
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
public class SQLOnPersonStream {
    public  SQLOnPersonStream() {
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: SQLOnPersonStream <in> <out>");
            System.exit(1);
        }

        // create streaming context
        Duration batchInterval = new Duration(20000);
        SparkConf sparkConf = new SparkConf().setAppName("SQLOnPersonStream");
        SparkConfUtil.setConf(sparkConf);
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);

        // create text file stream
        JavaDStream<String> textStream = ssc.textFileStream(args[0]);

        // create the Person DStream
        JavaDStream<Person> personStream = textStream.map(new Function<String, Person>() {
            @Override
            public Person call(String str) {
                Person p = new Person(str);
                return p;
            }
        });

        // query person dstream
        queryPersonStream(ssc, personStream);

        // join the streams
        JavaDStream<Person> jdstream = joinStreams(ssc, personStream);

        // query the joined stream
        queryJoinedStream(ssc, jdstream);

        ssc.start();
        ssc.awaitTermination();
    }

    // join the streams
    private static JavaDStream<Person> joinStreams(JavaStreamingContext ssc, JavaDStream<Person> personStream) {
        JavaPairDStream<String, Person> personpair = personStream.mapToPair(new PairFunction<Person, String, Person>() {
            @Override
            public Tuple2<String, Person> call(Person p) {
                return new Tuple2<String, Person>(p.getName(), p);
            }
        });

        // join the incoming streams
        JavaPairDStream<String, Tuple2<Person, Person>> joinStream = personpair.join(personpair);

        joinStream.print();

        // map it to a joined dataset
        JavaDStream<Person> jdstream = joinStream.map(new Function<Tuple2<String, Tuple2<Person, Person>>, Person>() {
            @Override
            public Person call(Tuple2<String, Tuple2<Person, Person>> t2) {
                Person p = t2._2()._2();
                return p;
            }
        });

        /***
         // map it to a joined dataset
         JavaPairDStream<String, Person> jdstream111 = joinStream.mapValues(new Function<Tuple2<Person, Person>, Person>() {
        @Override
        public Person call(Tuple2<Person, Person> t2) {
        Person p = t2._1();
        return p;
        }
        });
         ***/

        return jdstream;
    }

    // query the joined stream
    private static void queryJoinedStream(JavaStreamingContext ssc, JavaDStream<Person> joinedStream) {

        // Convert RDDs of the words DStream to DataFrame and run SQL query
        joinedStream.foreachRDD(new Function2<JavaRDD<Person>, Time, Void>() {
            @Override
            public Void call(JavaRDD<Person> rdd, Time time) {
                SQLContext sqlContext = SQLContextSingleton.getInstance(rdd.context());

                // Convert JavaRDD[Person] to DataFrame
                DataFrame personDataFrame = sqlContext.createDataFrame(rdd, Person.class);

                // Register as table
                personDataFrame.registerTempTable("persons");

                long count = personDataFrame.count();
                if (count > 0) {
                    System.out.println(""+count);
                }

                // Do word count on table using SQL and print it
                DataFrame personsCountsDataFrame =
                        sqlContext.sql("select name, count(*) as total from persons group by name");
                System.out.println("========= " + time + "=========");
                personsCountsDataFrame.show();

                personsCountsDataFrame =
                        sqlContext.sql("select name, count(*) as total from persons group by name");
                System.out.println("========= " + time + "=========");
                personsCountsDataFrame.show();

                return null;
            }
        });

    }

    // query Person DStream
    private static void queryPersonStream(JavaStreamingContext ssc, JavaDStream<Person> personStream) {

        // Convert RDDs of the words DStream to DataFrame and run SQL query
        personStream.foreachRDD(new Function2<JavaRDD<Person>, Time, Void>() {
            @Override
            public Void call(JavaRDD<Person> rdd, Time time) {
                SQLContext sqlContext = SQLContextSingleton.getInstance(rdd.context());

                // Convert JavaRDD[Person] to DataFrame
                DataFrame personDataFrame = sqlContext.createDataFrame(rdd, Person.class);

                // Register as table
                personDataFrame.registerTempTable("persons");

                // Do word count on table using SQL and print it
                DataFrame personsCountsDataFrame =
                        sqlContext.sql("select name, count(*) as total from persons group by name");
                System.out.println("========= " + time + "=========");
                personsCountsDataFrame.show();

                return null;
            }
        });
    }

}


