package com.cep.streaming.processor;

import com.cep.streaming.dataset.Stock;
import com.cep.streaming.event.CEPEvent;
import com.cep.streaming.listener.EventListener;
import com.cep.streaming.listener.MapEvent;
import com.cep.streaming.rule.StreamingRule;
import com.cep.streaming.sql.SQLContextSingleton;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by jayant on 7/20/15.
 */
public class ProcessStreamsAndEvaluateRules {

    public static void process(JavaStreamingContext ssc, JavaDStream<String> textStream,
                               String eventClass, final String tableName, final StreamingRule[] rules) throws Exception {

        final Class ec = Class.forName(eventClass);

        // create the specific CEPEvent DStream
        JavaDStream<CEPEvent> eventStream = textStream.map(new Function<String, CEPEvent>() {
            @Override
            public CEPEvent call(String str) {
                try {
                    CEPEvent event = (CEPEvent)ec.newInstance();
                    event.init(str);
                    return event;
                } catch(Exception ex) {
                    return null;
                }
            }
        });


        // Convert RDDs of the words DStream to DataFrame and run SQL query
        eventStream.foreachRDD(new Function2<JavaRDD<CEPEvent>, Time, Void>() {
            @Override
            public Void call(JavaRDD<CEPEvent> rdd, Time time) {
                SQLContext sqlContext = SQLContextSingleton.getInstance(rdd.context());

                // Convert JavaRDD[Stock] to DataFrame
                DataFrame stockDataFrame = sqlContext.createDataFrame(rdd, ec);

                // Register as table
                stockDataFrame.registerTempTable(tableName);

                for (int i=0; i<rules.length; i++) {
                    StreamingRule rule = rules[i];

                    DataFrame df = sqlContext.sql(rule.sql);
                    df.show();

                    try {
                        final EventListener listener = EventListener.createListener(Class.forName(rule.eventListenerClass));

                        df.javaRDD().map(new Function<Row, MapEvent>() {
                            @Override
                            public MapEvent call(Row row) throws Exception {

                                MapEvent event = new MapEvent();
                                for (int i=0; i<row.size(); i++) {
                                    Object o = row.get(i);
                                    event.put(i, o);
                                }
                                return event;
                            }
                        });

                    } catch(Exception ex) {

                    }

                }

                // #ticks/avg/min/max events per symbol on table using SQL and print it
                DataFrame stocksCountsDataFrame =
                        sqlContext.sql("select symbol, count(*) as numticks, avg(price) as avgprice, min(price) as minprice, max(price) as maxprice from stocks group by symbol");
                System.out.println("========= " + time + "=========");
                stocksCountsDataFrame.show();

                // create listener
                try {
                    final EventListener listener = EventListener.createListener(Class.forName("StockEventListener"));

                    stocksCountsDataFrame.javaRDD().map(new Function<Row, MapEvent>() {
                        @Override
                        public MapEvent call(Row row) throws Exception {
                            MapEvent event = new MapEvent();
                            for (int i=0; i<row.size(); i++) {
                                Object o = row.get(i);
                                event.put(i, o);
                            }
                            return event;
                        }
                    });

                    // find symbols with rapid drop off (> 20%)
                    DataFrame dropoffsDataFrame =
                            sqlContext.sql("select symbol, (max(price) - min(price))/min(price)*100 as percent_dropoff from stocks where percent_dropoff > 20 group by symbol");
                    System.out.println("========= " + time + "=========");
                    stocksCountsDataFrame.show();

                    // find the 20 min moving average

                    // detect stocks with 5 continuously increasing ticks

                } catch(Exception ex) {

                }

                return null;
            }
        });
    }

}
