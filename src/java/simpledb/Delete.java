package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId transactionId;

    OpIterator child;

    int called = 0;
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
        this.transactionId = t;
        this.child = child;
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
        child.open();   // child iterate tuples that be delete.
        super.open();   // result tuple iterator
    }

    public void close() {
        // some code goes here
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
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
        // some code goes here
        if(called == 1){
            return null;
        }
        called = 1;

        int count = 0;
        while(child.hasNext()){
            try {
                Database.getBufferPool().deleteTuple(transactionId, child.next());
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
        child = children[0];
    }

}
