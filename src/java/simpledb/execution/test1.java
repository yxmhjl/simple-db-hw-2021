package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class test1 extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int afield;
    private int gfield;
    private TupleDesc tupledesc;
    private Aggregator.Op aop;
    private Aggregator aggregator;
    private OpIterator resultIterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public test1(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        // Determine the TupleDesc for the output
        if (gfield >= 0) {
            Type[] types = new Type[2];
            types[0] = child.getTupleDesc().getFieldType(gfield);
            types[1] = Type.INT_TYPE; // Assume all aggregations return an integer
            tupledesc = new TupleDesc(types);
        } else {
            tupledesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        return gfield >= 0 ? child.getTupleDesc().getFieldName(gfield) : null;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        super.open();
        child.open();

        // Initialize the aggregator
        if (child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(gfield, gfield >= 0 ? child.getTupleDesc().getFieldType(gfield) : null, afield, aop);
        } else {
            aggregator = new StringAggregator(gfield, gfield >= 0 ? child.getTupleDesc().getFieldType(gfield) : null, afield, aop);
        }

        // Process all tuples from the child iterator
        while (child.hasNext()) {
            Tuple tuple = child.next();
            aggregator.mergeTupleIntoGroup(tuple);
        }

        // Create the iterator for the aggregated results
        resultIterator = aggregator.iterator();
        resultIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (resultIterator.hasNext()) {
            return resultIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        open(); // Reinitialize the aggregator and result iterator
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        return tupledesc;
    }

    public void close() {
        super.close();
        child.close();
        if (resultIterator != null) {
            resultIterator.close();
        }
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length != 1) {
            throw new IllegalArgumentException("Aggregate expects exactly one child");
        }
        this.child = children[0];
    }
}