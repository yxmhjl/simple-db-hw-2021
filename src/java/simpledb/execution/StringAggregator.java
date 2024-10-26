package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private ConcurrentHashMap<Field, GroupCalResult> groupResults;

    public class GroupCalResult {
        private int count = 0;

        public void addValue() {
            count++;
        }

        public int getCount() {
            return count;
        }
    }

    /**
     * Aggregate constructor
     *
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Only COUNT operation is supported for StringAggregator");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        this.groupResults = new ConcurrentHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupByField = gbfield >= 0 ? tup.getField(gbfield) : null;
        StringField value = (StringField) tup.getField(afield);
        if(groupByField == null) {
            groupByField = new IntField(NO_GROUPING);
        }
        GroupCalResult result = groupResults.get(groupByField);
        if (result == null) {
            result = new GroupCalResult();
            groupResults.put(groupByField, result);
        }
        result.addValue();
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new StringAggregatorIterator(groupResults, gbfield, gbfieldtype);
    }

    private class StringAggregatorIterator implements OpIterator {

        private final Map<Field, GroupCalResult> groupResults;
        private Iterator<Map.Entry<Field, GroupCalResult>> iterator;
        private final TupleDesc tupleDesc;
        private final boolean isGrouped;

        public StringAggregatorIterator(Map<Field, GroupCalResult> groupResults, int gbfield, Type gbfieldtype) {
            this.groupResults = groupResults;
            this.iterator = groupResults.entrySet().iterator();
            this.isGrouped = gbfield >= 0;
            this.tupleDesc = createTupleDesc(isGrouped, gbfieldtype);
        }

        private TupleDesc createTupleDesc(boolean isGrouped, Type gbfieldtype) {
            if (isGrouped) {
                return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            } else {
                return new TupleDesc(new Type[]{Type.INT_TYPE});
            }
        }

        @Override
        public void open(){
            // 打开迭代器时不需要额外操作
        }

        @Override
        public boolean hasNext(){
            return iterator.hasNext();
        }

        @Override
        public Tuple next(){
            Map.Entry<Field, GroupCalResult> entry = iterator.next();
            Field groupValue = entry.getKey();
            GroupCalResult result = entry.getValue();

            int count = result.getCount();

            Tuple tuple = new Tuple(tupleDesc);
            if (isGrouped) {
                tuple.setField(0, groupValue);
                tuple.setField(1, new IntField(count));
            } else {
                tuple.setField(0, new IntField(count));
            }
            return tuple;
        }

        @Override
        public void rewind(){
            iterator = groupResults.entrySet().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tupleDesc;
        }

        @Override
        public void close() {
            // 关闭迭代器时不需要额外操作
        }
    }
}