package simpledb;

import java.util.ArrayList;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    public ArrayList<Integer> buckets, left, right;

    public int min, max, width, ntuples;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.min = min;
        this.max = max;
        int inteval = max - min + 1;
        this.width = inteval / buckets + (inteval % buckets == 0 ? 0 : 1);
        this.ntuples = 0;
        this.buckets = new ArrayList<Integer>(buckets);
        this.left = new ArrayList<>(buckets);
        this.right = new ArrayList<>(buckets);
        for(int i = 0; i < buckets; ++ i){
            this.buckets.add(0);
            this.left.add(min + i * width);
            this.right.add(min + (i+1)*width); // [left, right)
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int idx = (v - min) / width;
        if(idx >= this.buckets.size() || idx < 0){
            return ;
        }
        int value = this.buckets.get(idx);
        ++ value;
        this.buckets.set(idx, value);
        this.ntuples += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int idx = (v - min) / width;
        int value = idx < 0 || idx >= this.buckets.size() ? 0 : this.buckets.get(idx);
        int allless = 0, alllarge = 0;
        for(int i = 0; i < idx ; ++ i){
            if(i < this.buckets.size()) {
                allless = allless + this.buckets.get(i);
            }
        }
        for(int i = idx+1; i < buckets.size(); ++ i){
            if(i >= 0) {
                alllarge = alllarge + this.buckets.get(i);
            }
        }
        double have = 0;

        switch (op){
            case LIKE:
                break;
            case EQUALS: {
                return (1.0 * value / width) / ntuples;
            }
            case NOT_EQUALS: {
                return 1 - (1.0 * value / width) / ntuples;
            }
            case LESS_THAN:{
                if(idx >= 0 && idx < this.buckets.size()){
                    have = this.buckets.get(idx);
                    have = 1.0 * have * (v - left.get(idx)) / width / ntuples;
                }
                return have + 1.0 * allless / ntuples;
            }
            case LESS_THAN_OR_EQ: {
                if(idx >= 0 && idx < this.buckets.size()) {
                    have = this.buckets.get(idx);
                    have = 1.0 * have * (v - left.get(idx) + 1) / width / ntuples;
                }
                return have + 1.0 * allless / ntuples;
            }
            case GREATER_THAN:{
                if(idx >= 0 && idx < this.buckets.size()){
                    have = this.buckets.get(idx);
                    have = 1.0 * have * (right.get(idx) - v - 1) / width / ntuples;
                }
                return have + 1.0 * alllarge / ntuples;
            }
            case GREATER_THAN_OR_EQ: {
                if(idx >= 0 && idx < this.buckets.size()){
                    have = this.buckets.get(idx);
                    have = 1.0 * have * (right.get(idx) - v) / width / ntuples;
                }
                return have + 1.0 * alllarge / ntuples;
            }
        }

    	// some code goes here
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String str = "";
        for(int i = 0; i < this.buckets.size(); i ++){
            str += String.valueOf(this.buckets.get(i)) + ",";
        }
        return str;
    }
}
