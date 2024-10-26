package simpledb;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.SeqScan;
import simpledb.storage.HeapFile;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;

public class test {

    public static void main(String[] argv) {


        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc descriptor = new TupleDesc(types, names);


        HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);
        Database.getCatalog().addTable(table1, "test");


        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());

        try {

            f.open();
            System.out.println("SeqScan opened");

            int tupleCount = 0;
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println("Tuple: " + tup);
                tupleCount++;
            }

            f.close();
            System.out.println("SeqScan closed, total tuples: " + tupleCount);

            Database.getBufferPool().transactionComplete(tid);
            System.out.println("Transaction completed");
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }
    }
}