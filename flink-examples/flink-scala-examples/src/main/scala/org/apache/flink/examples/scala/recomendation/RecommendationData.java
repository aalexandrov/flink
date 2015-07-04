package org.apache.flink.examples.scala.recomendation;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Created by vassil on 14.06.15.
 */
public class RecommendationData {

    public static final String[] ITEMS = new String[] {
            "a b",
            "b c d e",
            "a b c d f",
            "a d f",
            "a b f",
            "a b f ab ced",
            "a b d f ef cj",
            "c a d b e f",
            "a f d b",
    };

    public static DataSet<String> getDefaultTextLineDataSet(ExecutionEnvironment env) {
        return env.fromElements(ITEMS);
    }
}
