package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    TransactionId tid;
    OpIterator[] child;
    TupleDesc tupleDesc;
    int tableId;
    //是否是第一次调用标志位
    boolean flagisFirst = true;
    Type[] type = new Type[1];

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.child = new OpIterator[1];
        this.child[0] = child;
        this.tableId = tableId;
        this.tupleDesc = getTupleDesc();
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        type[0] = Type.INT_TYPE;
        return new TupleDesc(type);
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException{
        // some code goes here
        Tuple resultTuple  = new Tuple(new TupleDesc(type));
        type[0] = Type.INT_TYPE;
        //记录插入的个数
        int num = 0;
        if(flagisFirst) {
            try {
                while (child[0].hasNext()) {
                    Tuple tuple = child[0].next();
                    // 将元组插入到缓冲池中
                    Database.getBufferPool().insertTuple(tid,tableId, tuple);
                    num++;
                }
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
