package org.apache.flink.examples.scala.recomendation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Created by vassil on 14.06.15.
 */
public class RecommendationData {

    public static final String[] ITEMS = new String[] {
            "A B C D E F",
            "A B C D G L",
            "A B D E",
            "A E",
    };

    public static DataSet<String> getDefaultTextLineDataSet(ExecutionEnvironment env) {
        return env.fromElements(ITEMS);
    }
}
