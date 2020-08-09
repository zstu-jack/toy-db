package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int groupByField;

    Type groupByType;

    int aggregateField;

    Op op;

    Tuple tuple;
    Integer sum = 0, count = 0, min = Integer.MAX_VALUE, max = Integer.MIN_VALUE; // without group field.
    Map<Field, Integer> mapSum, mapMin, mapMax, mapCount;

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
        // some code goes here
        this.groupByField = gbfield;
        this.groupByType = gbfieldtype;
        this.aggregateField = afield;
        this.op = what;
        tuple = null;
        mapMax = new HashMap<>();
        mapMin = new HashMap<>();
        mapSum = new HashMap<>();
        mapCount = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        // sum, min, max, count
        if(tuple == null){
            tuple = tup;
        }
        if(this.groupByField == Aggregator.NO_GROUPING){
            sum += ((IntField)tup.getField(this.aggregateField)).getValue();
            min = Math.min(min, ((IntField)tup.getField(this.aggregateField)).getValue());
            max = Math.max(max, ((IntField)tup.getField(this.aggregateField)).getValue());
            count += 1;
        }else{
            int find = 0;
            for (Field key : mapCount.keySet()) {
                if(key.equals(tup.getField(groupByField))){
                    find = 1;
                    break;
                }
            }
            Field groupField = tup.getField(groupByField);

            IntField field = (IntField)tup.getField(aggregateField);
            if(find == 0) {
                mapCount.put(groupField, 1);
                mapMin.put(groupField, field.getValue());
                mapMax.put(groupField, field.getValue());
                mapSum.put(groupField, field.getValue());
            }else{
                mapCount.put(groupField, 1 + mapCount.get(groupField).intValue());
                mapMin.put(groupField, Math.min(field.getValue() , mapMin.get(groupField).intValue()));
                mapMax.put(groupField, Math.max(field.getValue() , mapMax.get(groupField).intValue()));
                mapSum.put(groupField, field.getValue() + mapSum.get(groupField).intValue());
            }
        }
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
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        if(tuple != null) {
            if (this.groupByField == Aggregator.NO_GROUPING) {
                TupleDesc desc = new TupleDesc(new Type[]{Type.INT_TYPE});
                tuple = new Tuple(desc);
                if (op == Op.MIN) tuple.setField(0, new IntField(min));
                if (op == Op.MAX) tuple.setField(0, new IntField(max));
                if (op == Op.SUM) tuple.setField(0, new IntField(sum));
                if (op == Op.AVG) tuple.setField(0, new IntField(sum / (count == 0 ? 1 : count)));
                if (op == Op.COUNT) tuple.setField(0, new IntField(count));
                tuples.add(tuple);
            } else {
                TupleDesc desc = new TupleDesc(new Type[]{tuple.getTupleDesc().getFieldType(groupByField), Type.INT_TYPE});
                for (Field field : mapCount.keySet()) {
                    tuple = new Tuple(desc);
                    tuple.setField(0, field);
                    if (op == Op.MIN) tuple.setField(1, new IntField(mapMin.get(field)));
                    if (op == Op.MAX) tuple.setField(1, new IntField(mapMax.get(field)));
                    if (op == Op.SUM) tuple.setField(1, new IntField(mapSum.get(field)));
                    if (op == Op.AVG) tuple.setField(1, new IntField(mapSum.get(field)/ (mapCount.get(field) == 0 ? 1 : mapCount.get(field))));
                    if (op == Op.COUNT) tuple.setField(1, new IntField(mapCount.get(field)));
                    tuples.add(tuple);
                }
            }
        }
        // System.out.println(tuples);
        return new AggregatorIterator(tuples);
        //throw new UnsupportedOperationException("please implement me for lab2");
    }

}
