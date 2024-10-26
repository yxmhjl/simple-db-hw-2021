package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
    private final int numPages;
    private final ConcurrentHashMap<PageId,Page> pages = new ConcurrentHashMap<>();
    private LRUCache lruCache;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.lruCache = new LRUCache(numPages);
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
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        if(!(lruCache.map.containsKey(pid)))
        {
            //现在用LRUcache代表我们的缓冲池
            //因为在LRUcache中移除的页面在pages没有移除会导致内存一直变大
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = file.readPage(pid);
            lruCache.put(pid,page);
        }
        return lruCache.map.get(pid).value;

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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
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
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        updateBufferPool(f.insertTuple(tid, t), tid);
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
        updateBufferPool(file.deleteTuple(tid, t), tid);
    }

    /**
     * update:delete ; add
     *
     * @param updatePages 需要变为脏页的页列表
     * @param tid         the transaction to updating.
     */
    public void updateBufferPool(List<Page> updatePages, TransactionId tid) {
        for (Page page : updatePages) {
            page.markDirty(true, tid);
            // update bufferPool
            lruCache.put(page.getId(), page);
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
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        //在当前这种结构下不用实现，因为替换了缓冲池，如果将缓冲池和LRUcache的内容同步则要实现
        //实现方法可以用map()取出page，同步给pages
    }

    private static class LRUCache {
        int cap,size;
        ConcurrentHashMap<PageId,Node> map ;
        Node head = new Node(null ,null);
        Node tail = new Node(null ,null);

        public LRUCache(int capacity) {
            this.cap = capacity;
            map = new ConcurrentHashMap<>();
            head.next = tail;
            tail.prev = head;
            size = 0;
        }
        private class Node
        {
            Node prev;
            Node next;
            Page value;
            PageId key;
            public Node(Page value,PageId key) {
                this.key = key;
                this.value = value;
            }
        }
        public synchronized int getSize(){
            return this.size;
        }
        public void put(PageId pid,Page value)
        {
            if(map.containsKey(pid))
            {
                map.get(pid).value = value;
                map.get(pid).key = pid;
                down(pid);
            }
            else
            {
                //申请一个结点，放到最上面，并放入map
                Node newNode = new Node(value,pid);
                head.next.prev = newNode;
                newNode.next = head.next;
                newNode.prev = head;
                head.next = newNode;
                map.put(pid,newNode);
                //根据栈是否满决定是否移除页面
                if(size < cap)
                {
                    //不满，不移动。
                    size++;
                }
                else
                {
                    //移动非脏页，首先找到最后使用的非脏页
                    Node notdirtynode = tail.prev;
                    while(notdirtynode.value.isDirty() != null && notdirtynode!=head)
                    {
                        notdirtynode = notdirtynode.prev;
                    }
                    if(notdirtynode != head && notdirtynode != tail)
                    {
                        map.remove(notdirtynode.key);
                        notdirtynode.prev.next = notdirtynode.next;
                        notdirtynode.next.prev = notdirtynode.prev;
                    }
                }
            }
        }
        public void down(PageId pid)
        {
            map.get(pid).prev.next = map.get(pid).next;
            map.get(pid).next.prev = map.get(pid).prev;
            map.get(pid).next = head.next;
            head.next.prev = map.get(pid);
            head.next = map.get(pid);
            map.get(pid).prev = head;
        }
        public Page get(PageId pid)
        {
            if(map.containsKey(pid))
            {
                //将该页面提到最上面，在它上面的下降
                down(pid);
                return map.get(pid).value;
            }
            else
            {
                return null;
            }
        }
        public Set<Map.Entry<PageId, Node>> getEntrySet(){
            return map.entrySet();
        }
    }

}
