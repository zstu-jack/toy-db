package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    OpIterator opIterator;

    int aggregateField;

    int groupField;

    Aggregator.Op op;

    Aggregator aggregator;

    OpIterator aggregatorIterator;

//    StringAggregator stringAggregator;
//
//    IntegerAggregator integerAggregator;

    // SELECT name, SUM(score) as score_sum FROM score_table GROUP BY name
    // jk   math 10
    // jk   eng  20
    // lucy g    2

    // result:
    // jk   30
    // lucy 2

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.opIterator = child;
        this.aggregateField = afield;
        this.groupField = gfield;
        this.op = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	    return groupField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
	return opIterator.getTupleDesc().getFieldName(groupField);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	return aggregateField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	return opIterator.getTupleDesc().getFieldName(aggregateField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
        super.open();
        Type type = null;
        if(this.opIterator.getTupleDesc().getFieldType(aggregateField) == Type.INT_TYPE){
            if(groupField != Aggregator.NO_GROUPING) type = opIterator.getTupleDesc().getFieldType(groupField);
            aggregator = new IntegerAggregator(groupField, type, aggregateField, op);
        }else{
            if(groupField != Aggregator.NO_GROUPING) type = opIterator.getTupleDesc().getFieldType(groupField);
            aggregator = new StringAggregator(groupField, opIterator.getTupleDesc().getFieldType(groupField), aggregateField, op);
        }

        opIterator.open();
        while (opIterator.hasNext()){
            Tuple tuple = opIterator.next();
            aggregator.mergeTupleIntoGroup(tuple);
        }

        aggregatorIterator = aggregator.iterator();
        aggregatorIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */

    // result of computing the aggregate instead of computed result.
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        if(aggregatorIterator.hasNext()){
            return aggregatorIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        aggregatorIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	return opIterator.getTupleDesc();
    }

    public void close() {
	// some code goes here
        super.close();
        aggregatorIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
	return new OpIterator[]{opIterator};
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
        opIterator = children[0];
    }
    
}
