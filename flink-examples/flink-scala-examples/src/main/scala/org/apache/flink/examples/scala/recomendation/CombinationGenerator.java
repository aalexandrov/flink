package org.apache.flink.examples.scala.recomendation;

/**
 * Created by vassil on 14.06.15.
 */
import java.util.Arrays;
import java.util.Stack;

/**
 * This class represents a iterator which geos through the different possible combinations
 *
 */
public class CombinationGenerator {

    public static final int[] BLANK_COMBINATION = new int[] { Integer.MIN_VALUE };

    private int k;
    private int n;
    private int[] result;
    private Stack<Integer> stack;
    private int[] source;
    private int[] nextCombination;

    public CombinationGenerator() {
        this.stack = new Stack<Integer>();
    }

    /**
     * Reset the generator generate combinations of size k, with the given source.
     *
     * @param k Size of the combinations to generate.
     * @param source Source of the combinations to be generated.
     */
    public void reset(int k, int[] source) {
        this.k = k;
        n = source.length;
        this.source = source;
        result = new int[k];
        stack.removeAllElements();
        stack.push(0);
        nextCombination = getNextCombination();
        return;
    }

    /**
     * Checks if there is another combination which could be generated
     *
     * @return
     */
    public boolean hasMoreCombinations() {
        if (k == 0 || BLANK_COMBINATION == nextCombination || !hasNextCombination()) {
            return false;
        }
        return true;
    }

    /**
     * Return the next combination.
     *
     * @return int array representing the next combination.
     */
    public int[] next() {
        int[] result = nextCombination;
        nextCombination = getNextCombination();
        Arrays.sort(nextCombination);
        return result;
    }

    // Internal method to check if there are more combinations.
    private boolean hasNextCombination() {
        return stack.size() > 0;
    }

    // Internal method to generate the next combination. Assumes setup is valid
    // otherwise it returns a blank combination.
    private int[] getNextCombination() {
        while (k != 0 && hasNextCombination()) {
            int index = stack.size() - 1;
            int value = stack.pop();
            while (value < n) {
                result[index++] = value++;
                stack.push(value);
                if (index == k) {
                    int[]combination = new int[k];
                    for (int i = 0; i < k; i++) {
                        combination[i] = source[result[i]];
                    }
                    return combination;
                }
            }
        }
        return BLANK_COMBINATION;
    }

}