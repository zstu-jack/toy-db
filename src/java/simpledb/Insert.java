package simpledb;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;

    private OpIterator child;

    int tableId;

    int called = 0;

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
        this.transactionId = t;
        this.child = child;
        this.tableId = tableId;
        this.called = 0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here

        // attention here...
        // not child's tupledesc
        // a tupledesc that describle the number of tuples that have been success inserted.
        Type types[] = new Type[]{ Type.INT_TYPE};
        TupleDesc tupleDesc = new TupleDesc(types);
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.open();  // child iterate tuples that be insert.
        super.open();       // result tuple iterator
    }

    public void close() {
        // some code goes here
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.close();
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
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(this.called == 1){
            return null;
        }
        this.called = 1;

        int count = 0;
        while(child.hasNext()){
            try {
                Database.getBufferPool().insertTuple(transactionId, tableId, child.next());
                ++ count;
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE};
        TupleDesc tupleDesc = new TupleDesc(types);
        Tuple tuple = new Tuple(tupleDesc);
        tuple.setField(0, new IntField(count));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
