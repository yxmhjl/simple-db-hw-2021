package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;
    /**
     * 写在内部类的原因是：DbFileIterator is the iterator interface that all SimpleDB Dbfile should
     */
    private static final class HeapFileIterator implements DbFileIterator {
        private final HeapFile heapFile;
        private final TransactionId tid;

        /**
         * 存储了堆文件迭代器
         */
        private Iterator<Tuple> tupleIterator;
        private int index;

        public HeapFileIterator(HeapFile file,TransactionId tid){
            this.heapFile = file;
            this.tid = tid;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            index = 0;
            tupleIterator = getTupleIterator(index);
        }

        private Iterator<Tuple> getTupleIterator(int pageNumber) throws TransactionAbortedException, DbException{
            if(pageNumber >= 0 && pageNumber < heapFile.numPages()){
                HeapPageId pid = new HeapPageId(heapFile.getId(),pageNumber);
                HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }else{
                throw new DbException(String.format("heapFile %d  does not exist in page[%d]!", pageNumber,heapFile.getId()));
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tupleIterator == null) {
                return false;
            }

            while (!tupleIterator.hasNext()) {
                index++;
                if (index < heapFile.numPages()) {
                    tupleIterator = getTupleIterator(index);
                } else {
                    return false;
                }
            }
            return true;
        }
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(tupleIterator == null || !tupleIterator.hasNext()){
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
            tupleIterator = null;
        }

    }

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
        return this.file;
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
        //throw new UnsupportedOperationException("implement this");
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        RandomAccessFile raf = null;
        HeapPageId hpid = (HeapPageId) pid;
        int pno = hpid.getPageNumber();
        long offset = pno * BufferPool.getPageSize();
        byte[] data = new byte[BufferPool.getPageSize()];
        try{
            raf = new RandomAccessFile(file, "r");
            raf.seek(offset);
            raf.readFully(data);
            Page page = new HeapPage(hpid, data);
            return page;
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try{
                if(raf != null){
                    raf.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", hpid, pno));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pageId = page.getId();
        int pageNo = pageId.getPageNumber();
        int offset = pageNo * BufferPool.getPageSize();
        byte[] pageData = page.getPageData();

        RandomAccessFile file = new RandomAccessFile(this.file, "rw");
        file.seek(offset);
        file.write(pageData);
        file.close();

        page.markDirty(false, null);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int filesize = (int) file.length();
        int numPages = filesize / BufferPool.getPageSize();
        return numPages;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pages = new ArrayList<>();
        int flagIsInsert = 0;
        for(int i = 0; i < numPages(); i++){
            HeapPageId heapPageId = new HeapPageId(getId(),i);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_WRITE);
            if(heapPage.getNumEmptySlots() == 0)
            {
                //在暂时找不到空槽的时候释放锁
                Database.getBufferPool().unsafeReleasePage(tid,heapPageId);
                continue;
            }
            heapPage.insertTuple(t);
            pages.add(heapPage);
            return pages;
//            flagIsInsert = 1;
        }
        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(file,true));
        byte[] data = new byte[BufferPool.getPageSize()];
        data = HeapPage.createEmptyPageData();
        bw.write(data);
        bw.close();
        //缓存进缓冲池
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(getId(),numPages()-1),Permissions.READ_WRITE);
        p.insertTuple(t);
        pages.add(p);
        return pages;
//        HeapPageId pageId = new HeapPageId(getId(),numPages());
//        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
//        if(heapPage.getNumEmptySlots() != 0)
//        {
//            heapPage.insertTuple(t);
//            int i=0;
//            for( i=0;i<heapPage.getNumEmptySlots();i++){
//                if(!heapPage.isSlotUsed(i))
//                {
//                    heapPage.insertTuple(t);
//                }
//            }
//            RecordId recordId = new RecordId(heapPage.getId(),i);
//            t.setRecordId(recordId);
//            return Collections.singletonList(heapPage);
//        }
//        else
//        {
//            byte[] newEmptypagedata = HeapPage.createEmptyPageData();
//            HeapPage newEmptyPage = new HeapPage(pageId,newEmptypagedata);
//            RecordId recordId = new RecordId(newEmptyPage.getId(),0);
//            t.setRecordId(recordId);
//            return Collections.singletonList(heapPage);
//        }
        //return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pageId = t.getRecordId().getPageId();
        int tupleno = t.getRecordId().getTupleNumber();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        return Collections.singletonList(heapPage);
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return  new HeapFileIterator(this,tid);
    }

}

