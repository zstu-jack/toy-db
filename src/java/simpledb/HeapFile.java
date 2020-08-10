package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */

public class HeapFile implements DbFile {

    public class FileIterator implements DbFileIterator{

        private int curPageNo;

        private HeapPage curPage;

        private TransactionId transactionId;

        private int tableId;

        private Iterator<Tuple> tupleIterator;

        FileIterator(TransactionId transactionId) {
            this.transactionId = transactionId;
            this.tableId = getId();  // generator at runtime.
        }

        @Override
        public void open()throws TransactionAbortedException, DbException {
            this.curPageNo = 0;
            HeapPageId heapPageId = new HeapPageId(tableId, curPageNo);
            curPage = (HeapPage) Database.getBufferPool().getPage(transactionId, heapPageId, Permissions.READ_ONLY);
            tupleIterator = curPage.iterator();

        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(tupleIterator == null){
                return false;
            }
            if(tupleIterator.hasNext()){
                return true;
            }
            while (curPageNo+1 < numPages()){
                curPageNo += 1;
                HeapPageId heapPageId = new HeapPageId(tableId, curPageNo);
                curPage = (HeapPage) Database.getBufferPool().getPage(transactionId, heapPageId, Permissions.READ_ONLY);
                tupleIterator = curPage.iterator();
                if(tupleIterator.hasNext()){
                    return true;
                }
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(tupleIterator == null){
                throw new NoSuchElementException();
            }
            if(!tupleIterator.hasNext()){
                throw new NoSuchElementException();
            }
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            curPageNo = 0;
            tupleIterator = null;
            curPage = null;

        }
    }

    public File file;

    public TupleDesc tupleDesc;



    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsolutePath().hashCode() % 100000;
        // todo: fixme
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        Page page = null;
        try {
            FileInputStream fis = new FileInputStream(file);
            byte[] b = new byte[BufferPool.getPageSize()];
            fis.skip(BufferPool.getPageSize() * pid.getPageNumber());
            // bug: fis.read offset indicate that offset of `b`.
            fis.read(b, 0, BufferPool.getPageSize());
            HeapPageId heapPageId = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            page = new HeapPage(heapPageId, b);
        }catch (IOException e){
            e.printStackTrace();
        }

        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile r = new RandomAccessFile(file, "rw");
        r.seek(BufferPool.getPageSize() * page.getId().getPageNumber());
        r.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pages = new ArrayList<>();
        for(int i = 0; i < numPages(); ++ i){
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i),Permissions.READ_WRITE);
            if(page.getNumEmptySlots() == 0){
                // TODO: should i add page to pages?
                // pages: An ArrayList contain the pages that were modified
                continue;
            }
            page.insertTuple(t);
            pages.add(page);
            return pages;
        }
        // todo: or may put in BufferPool and call read. then create new Page?
        // now create here;
        byte[] b = new byte[BufferPool.getPageSize()];
        for(int i = 0; i < BufferPool.getPageSize(); ++ i){
            b[i] = 0;
        }
        HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPages()), b);
        newPage.insertTuple(t);
        pages.add(newPage);
        writePage(newPage);
        Database.getBufferPool().getPage(tid, new HeapPageId(getId(), numPages()-1),Permissions.READ_WRITE);
        return pages;

    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes
        // not necessary for lab1
        ArrayList<Page> pages = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        pages.add(page);
        // disk access's control in BufferPools.
        // but write to disk in this class.
        return pages;

    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new FileIterator(tid);
    }



}

