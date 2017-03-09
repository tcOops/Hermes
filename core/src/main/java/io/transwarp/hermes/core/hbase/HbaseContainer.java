package io.transwarp.hermes.core.hbase;

import io.transwarp.hermes.common.model.HbaseModel;
import io.transwarp.hermes.persistence.raw.OracleConnectionPool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by rejudgex on 3/7/17.
 */
public class HbaseContainer {
    private static ArrayBlockingQueue<HbaseModel> putQueue;
    private static ArrayBlockingQueue<HbaseModel> updQueue;
    private static ArrayBlockingQueue<HbaseModel> deleteQueue;
    private static HbaseContainer hbaseContainer = null;
    private static final int HBASE_OPERATION_MAX_SIZE = 50;

    public HbaseContainer() {
        putQueue = new ArrayBlockingQueue<HbaseModel>(HBASE_OPERATION_MAX_SIZE);
        updQueue = new ArrayBlockingQueue<HbaseModel>(HBASE_OPERATION_MAX_SIZE);
        deleteQueue = new ArrayBlockingQueue<HbaseModel>(HBASE_OPERATION_MAX_SIZE);
    }

    public static synchronized HbaseContainer getInstance() {
        if(hbaseContainer == null) {
            hbaseContainer = new HbaseContainer();
        }
        return hbaseContainer;
    }

    public void push2PutQueue(HbaseModel hbaseModel) {
        try {
            putQueue.put(hbaseModel);
            System.out.println("put putQueue " + putQueue.size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public HbaseModel fetchHbaseFromPutQueue() {
        try {
            System.out.println("take putQueue " + putQueue.size());
            return putQueue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void push2UpdQueue(HbaseModel hbaseModel) {
        try {
            updQueue.put(hbaseModel);
            System.out.println("put updQueue" + updQueue.size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public HbaseModel fetchHbaseFromUpdQueue() {
        try {
            System.out.println("take updQueue" + updQueue.size());
            return updQueue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void push2DeleteQueue(HbaseModel hbaseModel) {
        try {
            deleteQueue.put(hbaseModel);
            System.out.println("put " + deleteQueue.size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public HbaseModel fetchHbaseFromDeleteQueue() {
        try {
            System.out.println("take " + deleteQueue.size());
            return deleteQueue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ArrayBlockingQueue<HbaseModel> getPutQueue() { return putQueue; }

    public ArrayBlockingQueue<HbaseModel> getUpdQueue() {
        return updQueue;
    }

    public ArrayBlockingQueue<HbaseModel> getDeleteQueue() { return deleteQueue; }
}
