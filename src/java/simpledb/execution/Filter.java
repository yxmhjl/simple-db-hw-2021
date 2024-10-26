package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate p;
    private OpIterator[] child;
    private TupleDesc td;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.p = p;
        this.child = new OpIterator[1];
        this.child[0] = child;
        this.td = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        // some code goes here
        return p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child[0].open();
    }

    public void close() {
        // some code goes here
//        child.close();
        super.close();
        child[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
//        child.rewind();
//        super.rewind();
        child[0].rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
//        Tuple tuple= new Tuple(td);
//        if(p.filter(tuple))
//        {
//            return child[0].next();
//        }
        while (child[0].hasNext()) {
            Tuple tuple = child[0].next();
            if(p.filter(tuple))
            {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return child;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child = children;
    }

}
