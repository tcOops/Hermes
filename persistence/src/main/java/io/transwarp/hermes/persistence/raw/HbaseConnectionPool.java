package io.transwarp.hermes.persistence.raw;

import org.apache.hadoop.hbase.client.Connection;

import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import io.transwarp.hermes.common.model.HbaseStatus;

/**
 * Created by rejudgex on 3/1/17.
 */
public class HbaseConnectionPool {
    protected static ConcurrentHashMap<String, HbaseConnection> idelHbaseConntions = null;
    protected static ConcurrentHashMap<String, HbaseConnection> activeHbaseConnections = null;
    protected static int initSize;
    protected static int maxSize;
    protected static int defaultInitSize = 40;
    protected static int defaultMaxSize = 40;
    protected static AtomicInteger idelSize = new AtomicInteger(0);
    protected static AtomicInteger activeSize = new AtomicInteger(0);
    protected static HbaseConnectionPool instance = null;

    protected static Lock lock = new ReentrantLock();
    protected static volatile boolean isShutdown = false;
    protected Object obj = new Object();

    private HbaseConnectionPool(int initSize, int maxSize) {
        this.initSize = initSize;
        this.maxSize = maxSize;
        idelHbaseConntions = new ConcurrentHashMap<String, HbaseConnection>();
        activeHbaseConnections = new ConcurrentHashMap<String, HbaseConnection>();
        initConnections();
    }

    public HbaseConnection getConnection() {
        if(isShutdown) {
            throw new RuntimeException("Hbase Pool has shuted down");
        }

        lock.lock();
        try {
            if(idelSize.get() > 0) {
                if(idelHbaseConntions.size() <= 0) {
                    throw new RuntimeException("");
                }
                Entry<String, HbaseConnection> entry = idelHbaseConntions.entrySet().iterator().next();
                String key = entry.getKey();
                HbaseConnection hbaseConn = entry.getValue();
                idelHbaseConntions.remove(key);
                idelSize.decrementAndGet();
                if(hbaseConn.getHbaseConnection().isClosed()) {
                    return getConnection();
                }
                activeHbaseConnections.put(key, hbaseConn);
                activeSize.incrementAndGet();
                return hbaseConn;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }

        if(activeSize.get() > maxSize) {
            throw new RuntimeException("Active Number exceed");
        }
        if(activeSize.get() >= maxSize) {
            synchronized (obj) {
                try {
                    obj.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return getConnection();
        }

        if(isShutdown) {
            throw new RuntimeException("pool has shuted down");
        }

        Connection conn = HbaseUtils.getConnection();
        String id = UUID.randomUUID().toString();
        HbaseConnection entity = new HbaseConnection();
        entity.setHbaseId(id);
        entity.setHbaseConnection(conn);
        entity.setHbaseStatus(HbaseStatus.active);
        activeHbaseConnections.put(id, entity);
        activeSize.incrementAndGet();
        return entity;
    }

    private void initConnections() {
        for(int i = 0; i < this.initSize; ++i) {
            HbaseConnection hbaseConn = new HbaseConnection();
            String id = UUID.randomUUID().toString();
            hbaseConn.setHbaseId(id);
            Connection conn = HbaseUtils.getConnection();
            if(conn == null) {
                continue;
            }

            hbaseConn.setHbaseConnection(conn);
            hbaseConn.setHbaseStatus(HbaseStatus.idel);
            idelHbaseConntions.put(id, hbaseConn);
            idelSize.getAndAdd(1);

            System.out.println("Creating Connection Pool now .... " + i);
        }
        System.out.println("Current: " + System.currentTimeMillis());
    }

    public static HbaseConnectionPool getInstance() {
        if(isShutdown) {
            throw new RuntimeException("Pool has Already Shuted down");
        }
        if(instance != null) {
            return instance;
        }
        return getInstance(defaultInitSize, defaultMaxSize);
    }

    public static HbaseConnectionPool getInstance(int initSize, int maxSize) {
        if(isShutdown) {
            throw new RuntimeException("Pool has Already Shuted down");
        }
        if(initSize < 0 || maxSize < 1) {
            throw new RuntimeException("InitSize need >= 0, and MaxSize needs >= 1");
        }
        if(initSize > maxSize) {
            initSize = maxSize;
        }

        synchronized (HbaseConnectionPool.class) {
            if(instance == null) {
                instance = new HbaseConnectionPool(initSize, maxSize);
            }

        }
        return instance;
    }

    public void releaseConnection(String id) {
        if(isShutdown) {
            throw new RuntimeException("Pool is shutdown");
        }
        if(idelSize.get() == maxSize) {
            HbaseUtils.closeConnection(activeHbaseConnections.remove(id).getHbaseConnection());
        }
        else {
            HbaseConnection hbaseConn = activeHbaseConnections.remove(id);
            hbaseConn.setHbaseStatus(HbaseStatus.idel);
            idelHbaseConntions.put(id, hbaseConn);
            idelSize.incrementAndGet();
            activeSize.decrementAndGet();
            synchronized (obj) {
                obj.notify();
            }
        }
    }

    public void shutdown() {
        isShutdown = true;
        synchronized (obj) {
            obj.notifyAll();
        }

        for(Entry<String, HbaseConnection> idelIt : idelHbaseConntions.entrySet()) {
            String key = idelIt.getKey();
            HbaseConnection hbaseConn = idelHbaseConntions.get(key);
            HbaseUtils.closeConnection((hbaseConn.getHbaseConnection()));
        }

        for(Entry<String, HbaseConnection> activeIt : idelHbaseConntions.entrySet()) {
            String key = activeIt.getKey();
            HbaseConnection entity = activeHbaseConnections.get(key);
            HbaseUtils.closeConnection(entity.getHbaseConnection());
        }

        initSize = 0;
        maxSize = 0;
        idelSize = new AtomicInteger(0);
        activeSize = new AtomicInteger(0);
    }

    public int getIdelSize() {
        return this.idelSize.get();
    }

    public int getActiveSize() {
        return this.activeSize.get();
    }
}
