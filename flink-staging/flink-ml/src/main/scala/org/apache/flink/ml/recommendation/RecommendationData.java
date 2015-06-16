package org.apache.flink.ml.recommendation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Created by vassil on 14.06.15.
 */
public class RecommendationData {

    public static final String[] ITEMS = new String[] {
            "1  1 2",
            "2  3 4 5",
            "3  3 1 6 4 2",
            "4  3 1 4 2",
            "5  1 6 4 2",
    };

    public static DataSet<String> getDefaultTextLineDataSet(ExecutionEnvironment env) {
        return env.fromElements(ITEMS);
    }
}
