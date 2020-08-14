package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    public static int numPages;

    public static Map<PageId, Page> pageID2Page;

    public static Map<PageId, Permissions> permissionsMap;
    public static Map<PageId, Set<TransactionId>> transactionIdMap;
    public static Map<TransactionId, Set<PageId>> transactionIdSetMap;
    public static Lock lock;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
//        pageID2Page = new ConcurrentHashMap<>();
//        permissionsMap = new ConcurrentHashMap<>();
//        transactionIdMap = new ConcurrentHashMap<>();
//        transactionIdSetMap = new ConcurrentHashMap<>();
        pageID2Page = new ConcurrentHashMap<>();
        permissionsMap = new HashMap<>();
        transactionIdMap = new HashMap<>();
        transactionIdSetMap = new HashMap<>();
        lock = new ReentrantLock();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

        // jps -l
        // jstack 11988

        // some code goes here

        try {
            int timeoutCount = 0;
            for(;;) {
                lock.lock();
                if (transactionIdMap.containsKey(pid)
                        && !transactionIdMap.get(pid).isEmpty()) {

                    if(permissionsMap.get(pid).permLevel == Permissions.READ_WRITE.permLevel){ // old one is exclusive lock.
                        if(transactionIdMap.get(pid).contains(tid)){
                            System.out.println(tid.toString() + "acquire lock again " +  Permissions.READ_WRITE.toString());
                            lock.unlock();
                            break;
                        }
                    }else{ // old one is shared lock.
                        if(perm.permLevel == Permissions.READ_ONLY.permLevel){ // now for read.
                            System.out.println(tid.toString() + "acquire lock " +  Permissions.READ_ONLY.toString());
                            transactionIdMap.get(pid).add(tid);

                            if(!transactionIdSetMap.containsKey(tid)) transactionIdSetMap.put(tid, new HashSet<>());
                            transactionIdSetMap.get(tid).add(pid);

                            lock.unlock();
                            break;
                        }else{ // upgrade to write
                            if(transactionIdMap.get(pid).size() == 1 && transactionIdMap.get(pid).contains(tid)){
                                System.out.println(tid.toString() + "upgrade lock to " +  Permissions.READ_WRITE.toString());
                                permissionsMap.put(pid, Permissions.READ_WRITE);
                                lock.unlock();
                                break;
                            }
                        }
                    }
                }else{
                    System.out.println(tid.toString() + "acquire lock as " +  perm.toString());
                    //System.out.println(tid.toString() + " acquire locks of page" + pid.toString());
                    transactionIdMap.put(pid, new HashSet<>());
                    transactionIdMap.get(pid).add(tid);
                    permissionsMap.put(pid, perm);

                    if(!transactionIdSetMap.containsKey(tid)) transactionIdSetMap.put(tid, new HashSet<>());
                    transactionIdSetMap.get(tid).add(pid);

                    lock.unlock();
                    break;
                }

                lock.unlock();
                Thread.sleep(10); // todo: use condition instead.
                if(++ timeoutCount >= 5){
                    throw new TransactionAbortedException();
                    // if dead lock. throw a exception to abort.
                    // user should commit/abort the transaction try again.
                }
            }
        } catch (InterruptedException e){
            e.printStackTrace();
        }


        if (pageID2Page.containsKey(pid)) {
            return pageID2Page.get(pid);
        }
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = file.readPage(pid);
        try {
            if (pageID2Page.size() >= numPages) {
                /*for (PageId key : pageID2Page.keySet()) {
                    flushPage(key);
                    break;
                }*/
                evictPage(); // fixme: evictPage instead of flush.
            }
        } catch (IOException e) {   // fixme: do not catch DbException("All pages are marked dirty...") here , throw to testcase
            e.printStackTrace();
        }
        pageID2Page.put(pid, page);
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lock.lock();

        System.out.println(tid.toString() + " release lock ");

        //System.out.println(tid.toString() + " release locks of page" + pid.toString());
        transactionIdMap.get(pid).remove(tid);
        transactionIdSetMap.get(tid).remove(pid);

        if(transactionIdMap.get(pid).size() == 0) transactionIdMap.remove(pid);
        if(transactionIdSetMap.get(tid).size() == 0) transactionIdSetMap.remove(tid);

        lock.unlock();
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        lock.lock();
        boolean result = transactionIdSetMap.get(tid).contains(p);
        lock.unlock();
        return result;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        lock.lock();
        if(commit) {
            Set<PageId> pageIds = transactionIdSetMap.get(tid);
            if(pageIds != null) {
                for (PageId pageId : pageIds) {
                    if (pageId == null) {
                        continue;
                    }
                    flushPage(pageId);
                    transactionIdMap.get(pageId).remove(tid);
                    if(transactionIdMap.get(pageId).size() == 0) transactionIdMap.remove(pageId);
                }
            }
            transactionIdSetMap.remove(tid);
            // fixme bug: transactionIdSetMap.get(tid) cause deadlock......
        }else{
            Set<PageId> pageIds = transactionIdSetMap.get(tid);
            if(pageIds != null) {
                for (PageId pageId : pageIds) {
                    if (pageId == null) {
                        continue;
                    }
//                    DbFile file = Database.getCatalog().getDatabaseFile(pageId.getTableId());
//                    Page page = file.readPage(pageId);
//                    pageID2Page.put(pageId, page);
//                    page.markDirty(false, null);
                    pageID2Page.remove(pageId);
                    transactionIdMap.get(pageId).remove(tid);
                    if(transactionIdMap.get(pageId).size() == 0) transactionIdMap.remove(pageId);
                }
            }
            transactionIdSetMap.remove(tid);
        }
        System.out.println(tid.toString() + "release lock ");
        lock.unlock();
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);

        // Todo: Bug: retrive all Page that is dirty.
        ArrayList<Page> affected = file.insertTuple(tid, t);
        for(int i = 0; i < affected.size(); ++ i) {
            affected.get(i).markDirty(true, tid);
            pageID2Page.put(affected.get(i).getId(), affected.get(i));
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> affected = file.deleteTuple(tid, t);
        for(int i = 0; i < affected.size(); ++ i) {
            affected.get(i).markDirty(true, tid);
            pageID2Page.put(affected.get(i).getId(), affected.get(i));
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(PageId key : pageID2Page.keySet()){
            flushPage(key);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageID2Page.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if(pageID2Page.containsKey(pid)) {
            Page page = pageID2Page.get(pid);
            if(page.isDirty() != null) {    // only flush dirty page.
                HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
                heapFile.writePage(page);
                page.markDirty(false, null);
            }
            // pageID2Page.remove(pid);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        lock.lock();
        Set<PageId> pageIdSet = transactionIdSetMap.get(tid);
        if (pageIdSet != null) {
            for (PageId p : pageIdSet) {
                flushPage(p);
            }
        }
        lock.unlock();
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException, IOException {
        // some code goes here
        // not necessary for lab1
        for(PageId pageId : pageID2Page.keySet()){
            if(pageID2Page.get(pageId).isDirty() != null){
                continue;
            }
            // flushPage(pageId);
            pageID2Page.remove(pageId);
            return ;
        }
        throw new DbException("All pages are marked dirty...");
    }

}
