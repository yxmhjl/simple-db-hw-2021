package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private TupleDesc tupleDesc;
    private OpIterator[] child;
    //是否是第一次调用标志位
    boolean flagisFirst = true;
    private Type[] types;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = new OpIterator[1];
        this.child[0] = child;
        types = new Type[1];
        types[0] = Type.INT_TYPE;
        this.tupleDesc = new TupleDesc(types);

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child[0].open();
    }

    public void close() {
        // some code goes here
        super.close();
        child[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child[0].rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //计数删除个数
        int num = 0;
        if(flagisFirst) {
            try {
                while (child[0].hasNext()) {
                    Tuple tuple = child[0].next();
                    Database.getBufferPool().deleteTuple(tid, tuple);
                    num++;
                }
                Tuple resultTuple = new Tuple(tupleDesc);
                resultTuple.setField(0, new IntField(num));
                flagisFirst = false;
                return resultTuple;
            } catch (IOException e) {
                // 处理 IOException
                throw new DbException("Error inserting tuple into buffer pool: " + e.getMessage());
            }
        }
        else
        {
            return null;
        }
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