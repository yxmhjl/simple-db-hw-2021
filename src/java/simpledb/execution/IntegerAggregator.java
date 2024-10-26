package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op operator;

    // 使用ConcurrentHashMap来存储聚合结果
    private ConcurrentHashMap<Field, GroupCalResult> groupResults;

    // 内部类用于存储每个分组的计算结果
    private static class GroupCalResult {
        private int sum;
        private int count;
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;

        public void addValue(int value) {
            sum += value;
            count++;
            if (value < min) min = value;
            if (value > max) max = value;
        }

        public int getSum() {
            return sum;
        }

        public int getCount() {
            return count;
        }

        public int getMin() {
            return min;
        }

        public int getMax() {
            return max;
        }

        public int getAverage() {
            if (count == 0) return 0; // 避免除以零
            return sum / count;
        }
    }

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.operator = what;
        this.groupResults = new ConcurrentHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupByField = gbfield >= 0 ? tup.getField(gbfield) : null;
        IntField aggField = (IntField) tup.getField(afield);
        int value = aggField.getValue();
        if(groupByField == null) {
            groupByField = new IntField(NO_GROUPING);
        }
        GroupCalResult result = groupResults.get(groupByField);
        if (result == null) {
            result = new GroupCalResult();
            groupResults.put(groupByField, result);
        }

        result.addValue(value);
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
        return new IntegerAggregatorIterator(groupResults, gbfield, gbfieldtype, operator);
    }

    private class IntegerAggregatorIterator implements OpIterator {

        private final Map<Field, GroupCalResult> groupResults;
        private Iterator<Map.Entry<Field, GroupCalResult>> iterator;
        private final TupleDesc tupleDesc;
        private final boolean isGrouped;
        private final Op operator;

        public IntegerAggregatorIterator(Map<Field, GroupCalResult> groupResults, int gbfield, Type gbfieldtype, Op operator) {
            this.groupResults = groupResults;
            this.iterator = groupResults.entrySet().iterator();
            this.isGrouped = gbfield >= 0;
            this.operator = operator;

            // 创建 TupleDesc 对象
            if (isGrouped) {
                this.tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            } else {
                this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
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

            int aggregateValue = 0;
            switch (operator) {
                case MIN:
                    aggregateValue = result.getMin();
                    break;
                case MAX:
                    aggregateValue = result.getMax();
                    break;
                case SUM:
                    aggregateValue = result.getSum();
                    break;
                case AVG:
                    aggregateValue = result.getAverage();
                    break;
                case COUNT:
                    aggregateValue = result.getCount();
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported aggregation operator: " + operator);
            }

            Tuple tuple = new Tuple(tupleDesc);

            if (isGrouped) {
                // 如果有分组，创建包含 (groupValue, aggregateValue) 的元组
                tuple.setField(0, groupValue);
                tuple.setField(1, new IntField(aggregateValue));
            } else {
                // 如果没有分组，创建只包含 aggregateValue 的元组
                tuple.setField(0, new IntField(aggregateValue));
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