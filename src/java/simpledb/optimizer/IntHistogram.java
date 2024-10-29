package simpledb.optimizer;

import simpledb.execution.Predicate;

import static simpledb.execution.Predicate.Op.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private bucket[] bucketArray;
    double sizebuckets;
    int totaltuples;

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
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.bucketArray = new bucket[buckets];
        totaltuples = 0;
        sizebuckets = Math.max(1,(1.0+max-min)/buckets);
        for (int i = 0; i < buckets-1; i++) {
            bucketArray[i] = new bucket();
            bucketArray[i].left = min + i * sizebuckets;
            bucketArray[i].rigjt = bucketArray[i].left + sizebuckets;
            bucketArray[i].numtuple = 0;
        }
        bucketArray[buckets-1] = new bucket();
        bucketArray[buckets-1].left = bucketArray[buckets-2].rigjt;
        bucketArray[buckets-1].rigjt = max;
        bucketArray[buckets-1].numtuple = 0;
    }

    private class bucket{
        double left;
        double rigjt;
        int numtuple;
        bucket(){
            left = 0;
            rigjt = 0;
            numtuple = 0;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if(v>=min && v<=max) {
            int index_bucket = (int) ((v-min)/sizebuckets);
            bucketArray[index_bucket].numtuple++;
            totaltuples++;
        }


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

    	// some code goes here
        int index_bucket = (int) ((v-min)/sizebuckets);
        int countfront = 0;
        int countnext = 0;
        double equalBase = 0.0;
        double lessBase = 0.0;
        double greatBase = 0.0;
        if(v>=min && v<=max) {
            equalBase = (double) bucketArray[index_bucket].numtuple/(bucketArray[index_bucket].rigjt-bucketArray[index_bucket].left)/totaltuples;
            for (int i = 0; i < index_bucket ; i++) {
                countfront = countfront+bucketArray[i].numtuple;
            }
            lessBase = (((double) bucketArray[index_bucket].numtuple/(bucketArray[index_bucket].rigjt-bucketArray[index_bucket].left))*(v-1-bucketArray[index_bucket].left)
                    + countfront)/totaltuples;
            for (int i = index_bucket+1; i < buckets ; i++) {
                countnext = countnext+bucketArray[i].numtuple;
            }
            greatBase = (((double) bucketArray[index_bucket].numtuple/(bucketArray[index_bucket].rigjt-bucketArray[index_bucket].left))*(bucketArray[index_bucket].rigjt-v-1)
                    + countnext)/totaltuples;
        }
        switch (op) {
            case LESS_THAN:
                if(v>max)
                {
                    return 1.0;
                }
                if(v<=min)
                {
                    return 0.0;
                }
                return lessBase;
            case GREATER_THAN:
                if(v>=max)
                {
                    return 0.0;
                }
                if(v<min)
                {
                    return 1.0;
                }
                return greatBase;
            case LESS_THAN_OR_EQ:
                return 1-estimateSelectivity(GREATER_THAN,v);
            case GREATER_THAN_OR_EQ:
                return 1-estimateSelectivity(LESS_THAN,v);
            case EQUALS:
                return equalBase;
            case NOT_EQUALS:
                return 1-estimateSelectivity(EQUALS,v);
            default:
        }
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
        return null;
    }
}
