package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.ArrayList;
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

    private static int MAX_SIZE = 20;
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private final int numPages;
    private final ConcurrentHashMap<PageId,Page> pages = new ConcurrentHashMap<>();
    private LRUCache lruCache;
    private LockManage lockManage;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.lruCache = new LRUCache(numPages);
        this.lockManage = new LockManage();
    }

    private static class LockManage{
        //定义一个哈希表，键值是要上锁的页，值为一个事务链表
        ConcurrentHashMap<PageId,transationlocklist> locks;
        private WaitforGraph waitforGraph;
        public LockManage() {
            locks = new ConcurrentHashMap<>();
            waitforGraph = new WaitforGraph();
        }

        //内部类等待依赖图
        public  class WaitforGraph{
            //存放事务的列表
            ArrayList<node> transationnode = new ArrayList<>();
            //存放事务之间的等待关系,默认初始化为0。
            int[][] waitRelation = new int[MAX_SIZE][MAX_SIZE];
            //一个哈希表将事务号转换成整形，方便存放
            ConcurrentHashMap<TransactionId,Integer> index = new ConcurrentHashMap<>();
            public void addrelation(TransactionId tid1,TransactionId tid2)
            {
                int index1 = -1;
                if(index.get(tid1)>=0);
                {
                    index1 = index.get(tid1);
                }
                int index2 = -1;
                if(index.get(tid2)>=0)
                {
                    index2 = index.get(tid2);
                }
                waitRelation[index1][index2] = 1;
            }
            public void addtransation(TransactionId tid,PageId pid,int locktype)
            {
                node tempnode = new node();
                tempnode.pid = pid;
                tempnode.locktype = locktype;
                tempnode.tid = tid;
                transationnode.add(tempnode);
                if(index.get(tid)==null)
                {
                    index.put(tid, transationnode.size());
                }
                //判断要加的边
                for (int i = 0; i < transationnode.size(); i++) {
                    node node1 = transationnode.get(i);
                    if(node1.pid == pid && node1.tid != tid)
                    {
                        if(node1.locktype == 1)
                        {
                            addrelation(node1.tid,tid);
                        }
                        else if(node1.locktype == 0)
                        {
                            if(locktype == 1)
                            {
                                addrelation(node1.tid,tid);
                            }
                        }
                    }
                }
                isDeadLock(tid);
            }
            public class node{
                TransactionId tid;
                PageId pid;
                int locktype;
            }
        }

        //加锁,
        public synchronized boolean lock(TransactionId tid,PageId pid,int lock_type)
        {
            //如果没有当前数据项的事务锁列表则创建
            if(!locks.containsKey(pid))
            {
                locks.put(pid, getheadandtaillist());
                locks.get(pid).key = pid;
            }
            //新建一个事务结点
            transationlocklist templocklistnode = new transationlocklist();
            //初始化
            templocklistnode.tid = tid;
            templocklistnode.locktype = lock_type;
            //将结点放进头尾指针中去
            //首先找到链表末尾
            transationlocklist tailnode;
            transationlocklist nownode = locks.get(pid);
            tailnode=gettailnode(nownode);
            templocklistnode.next = tailnode;
            tailnode.prev.next = templocklistnode;
            templocklistnode.prev = tailnode.prev;
            tailnode.prev = templocklistnode;
            //将当前的锁指令加入等待图看是否会有死锁，有的话则回滚
            waitforGraph.addtransation(tid,pid,lock_type);
            //是否可以加锁，如果可以则按照链表次序加锁，加锁成功返回true，加锁失败返回f
            if(!addlock(locks.get(pid)))
            {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            else
            {
                return  true;
            }
            return false;
        }

        //获取尾指针
        public transationlocklist gettailnode(transationlocklist nownode)
        {
            transationlocklist tailnode;
            while(true)
            {
                if(nownode.istail)
                {
                    tailnode = nownode;
                    return tailnode;
                }
                nownode = nownode.next;
            }
        }

        //解锁，同时考虑解锁后发生的可能的加锁
        public synchronized void unlock(TransactionId tid,PageId pid,int lock_type,boolean remove)
        {
            if(!locks.containsKey(pid))
            {
//              throw DbException;
            }
            else
            {
                transationlocklist templocklistnode = locks.get(pid).next;
                transationlocklist unlocknode;
                //找到要删除的结点
                while(!templocklistnode.istail)
                {
                    if(templocklistnode.tid == tid && templocklistnode.locktype == lock_type)
                    {
                        if(!templocklistnode.isalocked)
                        {
//                            throw DbException;
                        }
                        else
                        {
                            unlocknode = templocklistnode;
                            //因为是直接遍历求锁的状态，所以现在直接删除即可
                            unlocknode.prev.next = unlocknode.next;
                            unlocknode.next.prev = unlocknode.prev;
                            break;
                        }
                    }
                    templocklistnode = templocklistnode.next;
                }
                //删除该节点看是否可以加新的锁
                if(remove)
                {
                    notifyAll();
                    addlock(locks.get(pid));
                }
            }
        }

        //事务终止
        public void abort(TransactionId tid,int lock_type)
        {
            //删除每个数据项中该事务的s锁和x锁
            //先删除s锁，但不执行下一步的加锁
            //然后删除x锁，看本数据项的锁链表看是否能加锁
            for (Map.Entry<PageId, transationlocklist> entry : locks.entrySet()) {
                PageId pid = entry.getKey();
                unlock(tid, pid, 0, false);
                unlock(tid, pid, 1, true);
            }
        }

        //升级锁，当只有当前事务持有该数据项的s锁的时候能够升级
        public void uplock(TransactionId tid,PageId pid)
        {
            waitforGraph.addtransation(tid,pid,1);

            int slock = 0;
            int xlock = 0;
            transationlocklist templocklistnode = locks.get(pid).next;
            while(!templocklistnode.istail)
            {
                if(templocklistnode.isalocked)
                {
                    //上了s锁
                    if(templocklistnode.locktype == 0)
                    {
                        slock++;
                    }
                    //上了x锁
                    if(templocklistnode.locktype == 1)
                    {
                        xlock++;
                    }
                }
                templocklistnode = templocklistnode.next;
            }
            templocklistnode = locks.get(pid).next;
            while(!templocklistnode.istail)
            {
                if(templocklistnode.tid == tid)
                {
                    if(slock == 1 && templocklistnode.locktype == 0
                    && templocklistnode.isalocked == true && xlock == 0)
                    {
                        templocklistnode.locktype = 1;
                    }
                    else {
                        // 升级条件不满足
                    }
                }
                templocklistnode = templocklistnode.next;
            }
        }

        //判断是否有死锁lockManage.isDeadLock();
        public void isDeadLock(TransactionId tid){
            if(isCyclic())
            {
                try {
                    throw new TransactionAbortedException();
                } catch (TransactionAbortedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        // 检测环
        public boolean isCyclic() {
            boolean[] visited = new boolean[MAX_SIZE];
            boolean[] recStack = new boolean[MAX_SIZE];

            for (int i = 0; i < MAX_SIZE; i++) {
                if (isCyclicUtil(i, visited, recStack)) {
                    return true;
                }
            }
            return false;
        }

        // 深度优先搜索
        private boolean isCyclicUtil(int i, boolean[] visited, boolean[] recStack) {
            if (recStack[i]) {
                return true;
            }
            if (visited[i]) {
                return false;
            }
            visited[i] = true;
            recStack[i] = true;

            for (int j = 0; j < MAX_SIZE; j++) {
                if (waitforGraph.waitRelation[i][j] == 1) {
                    if (isCyclicUtil(j, visited, recStack)) {
                        return true;
                    }
                }
            }

            recStack[i] = false;
            return false;
        }

        //看一个事务在一个数据项上是否有锁
        public boolean hooldslock(TransactionId tid,PageId pid)
        {
            if(locks.get(pid)==null)
            {
                return false;
            }
            transationlocklist templocklistnode = locks.get(pid).next;
            while(!templocklistnode.istail)
            {
                if(templocklistnode.tid == tid && templocklistnode.isalocked == true)
                {
                    return true;
                }
                templocklistnode = templocklistnode.next;
            }
            return false;
        }

        //获取锁的类型
        public int locktype(TransactionId tid,PageId pid){
            transationlocklist templocklistnode = locks.get(pid).next;
            while(!templocklistnode.istail)
            {
                if(templocklistnode.tid == tid && templocklistnode.isalocked == true)
                {
                    return templocklistnode.locktype;
                }
                templocklistnode = templocklistnode.next;
            }
            return -1;
        }

        //看能不能继续加锁
        public boolean addlock(transationlocklist headnode)
        {
            int slock = 0;
            int xlock = 0;
            boolean iscanlock = false;
            transationlocklist templocklistnode = headnode.next;
            while(!templocklistnode.istail)
            {
                //判断是否上锁
                if(templocklistnode.isalocked)
                {
                    //上了s锁
                    if(templocklistnode.locktype == 0)
                    {
                        slock++;
                    }
                    //上了x锁
                    if(templocklistnode.locktype == 1)
                    {
                        xlock++;
                    }
                }
                else
                {
                    if(xlock != 0)
                    {
                        return false;
                    }
                    //暂未考虑退出遍历
                    if(templocklistnode.locktype == 0)
                    {
                        //此时已经保证没有互斥锁
                        templocklistnode.isalocked = true;
                        slock++;
                        iscanlock = true;
                    }
                    else
                    {
                        //xs锁同时为空
                        if(slock == 0)
                        {
                            templocklistnode.isalocked = true;
                            xlock++;
                            iscanlock = true;
                        }
                    }
                }
                templocklistnode = templocklistnode.next;
            }
            return iscanlock;
        }

        //事务列表，初始化时假设没有分配，locktype为锁的类型，0为s锁，1为x锁。
        public class transationlocklist{
            LockManage.transationlocklist prev;
            LockManage.transationlocklist next;
            PageId key;
            boolean isalocked = false;
            boolean istail = false;
            TransactionId tid;
            int locktype = -1;
            public transationlocklist(){
            }
        }
        //获取一个头尾项链的链表，用于每个页的锁链表的初始化。
        public transationlocklist getheadandtaillist(){
            LockManage.transationlocklist head;
            LockManage.transationlocklist tail;
            head = new transationlocklist();
            tail = new transationlocklist();
            tail.istail = true;
            head.prev = tail;
            tail.next = head;
            head.next = tail;
            tail.prev = head;
            return head;
        }
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
//        System.out.println(22);
        int locktype = -1;
        if(lockManage.hooldslock(tid, pid))
        {
            locktype = lockManage.locktype(tid,pid);
        }
        if(!(lruCache.map.containsKey(pid)))
        {
            //现在用LRUcache代表我们的缓冲池
            //因为在LRUcache中移除的页面在pages没有移除会导致内存一直变大
            //根据事务的处理需求加锁
            if (perm == Permissions.READ_ONLY)
            {
                //返回-1说明没有锁
                if(locktype==-1)
                {
                    lockManage.lock(tid,pid,0);
                }
            }
            else
            {
                if(locktype==0)
                {
                    lockManage.uplock(tid,pid);
                }
                else if(locktype==-1)
                {
                    lockManage.lock(tid,pid,1);
                }
            }
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = file.readPage(pid);
            lruCache.put(pid,page);
        }
        else
        {
            if (perm == Permissions.READ_ONLY)
            {
                //返回-1说明没有锁
                if(locktype==-1)
                {
                    lockManage.lock(tid,pid,0);
                }
            }
            else
            {
                if(locktype==0)
                {
                    lockManage.uplock(tid,pid);
                }
                else if(locktype==-1)
                {
                    lockManage.lock(tid,pid,1);
                }
            }
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
        lockManage.unlock(tid,pid,0,false);
        lockManage.unlock(tid,pid,1,true);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid){
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)  {
        // some code goes here
        // not necessary for lab1|lab2
        Set<Map.Entry<PageId, LRUCache.Node>> entrySet = lruCache.getEntrySet();
        //事务提交，与事务关联的脏页刷新到磁盘
        //要先将脏页写回磁盘，再释放锁
        if(commit)
        {
            //遍历缓冲池的页表把脏页写回磁盘Set<Map.Entry<PageId, Node>>
            for(Map.Entry<PageId, LRUCache.Node> entry:entrySet)
            {
                Page page = entry.getValue().value;
                DbFile heapFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                //lab56
                Page before = page.getBeforeImage();

                //将脏页写回磁盘
                try {
                    //lab6
                    flushPage(page.getId());
                    page.setBeforeImage();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                //把有的锁释放
                unsafeReleasePage(tid, entry.getKey());
            }
        }
        //回滚事务，先释放锁，再更新缓冲池的数据
        else {
            for(Map.Entry<PageId, LRUCache.Node> entry:entrySet)
            {
                //把持有的锁释放
                unsafeReleasePage(tid, entry.getKey());
                //脏页则重新读取
                if(entry.getValue().value.isDirty()!=null)
                {
                    //这里注意不能在缓冲池里请求而是磁盘上请求pagid这一页。
                    Page page = null;
                    DbFile file = Database.getCatalog().getDatabaseFile(entry.getKey().getTableId());
                    page = file.readPage(entry.getKey());
                    //在原位替换，而不是添加，添加可能会挤掉一些页面。
                    lruCache.map.get(entry.getKey()).value = page;
                }
            }
        }
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2.
        return  lockManage.hooldslock(tid,p);
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
        List<Page> pAge = file.deleteTuple(tid, t);
        updateBufferPool(pAge, tid);
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
            try {
                lruCache.put(page.getId(), page);
            } catch (DbException e) {
                throw new RuntimeException(e);
            }
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
        for (Map.Entry<PageId, LRUCache.Node> group : lruCache.getEntrySet()) {
            Page page = group.getValue().value;
            if (page.isDirty() != null) {
                this.flushPage(group.getKey());
            }
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

    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page target = lruCache.get(pid);
        if(target == null){
            return;
        }
        TransactionId tid = target.isDirty();
        if (tid != null) {
            Page before = target.getBeforeImage();
            Database.getLogFile().logWrite(tid, before,target);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(target);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Map.Entry<PageId, LRUCache.Node> group : this.lruCache.getEntrySet()) {
            PageId pid = group.getKey();
            Page flushPage = group.getValue().value;
            TransactionId flushPageDirty = flushPage.isDirty();
            Page before = flushPage.getBeforeImage();
            // 涉及到事务就应该setBeforeImage
            flushPage.setBeforeImage();
            if (flushPageDirty != null && flushPageDirty.equals(tid)) {
                Database.getLogFile().logWrite(tid, before, flushPage);
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(flushPage);

            }
        }
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
        //放入一个键值对
        public void put(PageId pid,Page value) throws DbException
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
                        //删除之前要释放锁，但是这里的事务可能不对
                        Database.getBufferPool().unsafeReleasePage(new TransactionId(),notdirtynode.key);
                    }
                    else
                    {
                        throw new DbException("没有非脏页");
                    }
                }
            }
        }
        //将对应的项下移
        public void down(PageId pid)
        {
            map.get(pid).prev.next = map.get(pid).next;
            map.get(pid).next.prev = map.get(pid).prev;
            map.get(pid).next = head.next;
            head.next.prev = map.get(pid);
            head.next = map.get(pid);
            map.get(pid).prev = head;
        }
        //取出一个对
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


    public void getnewpage(PageId pid)
    {
        Page page = null;
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        page = file.readPage(pid);
        //在原位替换，而不是添加，添加可能会挤掉一些页面。
        lruCache.map.get(pid).value = page;
    }
}

