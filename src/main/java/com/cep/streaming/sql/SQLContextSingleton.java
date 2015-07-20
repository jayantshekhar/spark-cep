package com.cep.streaming.sql;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jayant on 7/19/15.
 */

/** Lazily instantiated singleton instance of SQLContext */
public class SQLContextSingleton {

    static private transient SQLContext instance = null;

    static public SQLContext getInstance(SparkContext sparkContext) {
        if (instance == null) {
            instance = new SQLContext(sparkContext);
        }
        return instance;
    }

}
