package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int groupByField;

    Type groupByType;

    int aggregateField;

    Op op;

    // string only need implement count. without groupby field.
    Tuple tuple;
    int count;
    // string only need implement count.
    Map<Field, Integer> mapCount;

    class AggregatorIterator implements OpIterator{

        public ArrayList<Tuple> tuples;

        public Iterator<Tuple> iterator;

        public AggregatorIterator(ArrayList<Tuple> tuples){
            this.tuples = tuples;
            this.iterator = this.tuples.iterator();

        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.iterator = this.tuples.iterator();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return iterator.next();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iterator.hasNext();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.iterator = this.tuples.iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tuples.get(0).getTupleDesc();
        }

        @Override
        public void close() {
            this.iterator = this.tuples.iterator();
        }
    }

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupByField = gbfield;
        this.groupByType = gbfieldtype;
        this.aggregateField = afield;
        this.op = what;
        tuple = null;
        mapCount = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(tuple == null){
            tuple = tup;
        }
        if(this.groupByField == Aggregator.NO_GROUPING){
            ++ count;
        }else{
            for (Field key : mapCount.keySet()) {
                if(key.equals(tup.getField(groupByField))){
                    Integer result = mapCount.get(key);
                    mapCount.put(key, result+1);
                    return;
                }
            }
            Field field = tup.getField(groupByField);
            mapCount.put(field, 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */

    // only need to support count.
    // retrive result
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        if(this.groupByField == Aggregator.NO_GROUPING){
            TupleDesc desc = new TupleDesc(new Type[]{Type.INT_TYPE});
            tuple = new Tuple(desc);
            tuple.setField(0, new IntField(count));
            tuples.add(tuple);
        }else{
            TupleDesc desc = new TupleDesc(new Type[]{tuple.getTupleDesc().getFieldType(groupByField) ,Type.INT_TYPE});
            for(Field field : mapCount.keySet()){
                tuple = new Tuple(desc);
                tuple.setField(0, field);
                tuple.setField(1, new IntField(mapCount.get(field)));
                tuples.add(tuple);
            }
        }
        return new AggregatorIterator(tuples);
        // throw new UnsupportedOperationException("please implement me for lab2");
    }

// testcode.
//    @Test
//    public void mergeCount() throws Exception {
//        scan1.open();
//        StringAggregator agg = new StringAggregator(0, Type.INT_TYPE, 1, Aggregator.Op.COUNT);
//
//        for (int[] step : count) {
//            agg.mergeTupleIntoGroup(scan1.next());
//            OpIterator it = agg.iterator();
//            it.open();
//            TestUtil.matchAllTuples(TestUtil.createTupleList(width1, step), it);
//        }
//    }
}
