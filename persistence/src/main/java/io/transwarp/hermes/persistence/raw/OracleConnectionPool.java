package io.transwarp.hermes.persistence.raw;

/**
 * Created by rejudgex on 2/22/17.
 */
import java.io.InputStream;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.*;
import io.transwarp.hermes.common.common.Constants;

public class OracleConnectionPool extends Object{
    public static OracleConnectionPool connPool = null;

    private ArrayBlockingQueue<OracleConnection> freeConnQueue;
    private ArrayBlockingQueue<OracleConnection> activeConnQueue;

    private Lock  getConnectionLock;
    private Lock releaseConnectionLock;

    private OracleConnectionPool() {
        try {
            freeConnQueue = new ArrayBlockingQueue<OracleConnection>(Constants.ORACLE_CONN_COUNT);
            activeConnQueue = new ArrayBlockingQueue<OracleConnection>(Constants.ORACLE_CONN_COUNT);
            getConnectionLock = new ReentrantLock();
            releaseConnectionLock = new ReentrantLock();

            InputStream is = OracleConnection.class.getClassLoader().getResourceAsStream("config/secret.properties");
            Properties secretProps = new Properties();
            secretProps.load(is);

            Class.forName(secretProps.getProperty("ORACLE_DRIVER"));
            for(int i = 0; i < Constants.ORACLE_CONN_COUNT; ++i) {
                freeConnQueue.put(new OracleConnection(DriverManager.getConnection(
                        secretProps.getProperty("ORACLE_URL"), secretProps.getProperty("ORACLE_USERNAME"), secretProps.getProperty("ORACLE_PASSWORD")
                )));
                System.out.println("Oracle GetConnection : " + i);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static synchronized OracleConnectionPool getInstance() {
        if(connPool == null) {
            connPool = new OracleConnectionPool();
        }
        return connPool;
    }

    public OracleConnection getConnection() {
        try {
            getConnectionLock.lock();
            OracleConnection oracleConn = freeConnQueue.take();
            activeConnQueue.put(oracleConn);
            return oracleConn;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            getConnectionLock.unlock();
        }
        return null;
    }

    public void releaseConnection(OracleConnection oracleConn) {
        try {
            releaseConnectionLock.lock();
            activeConnQueue.remove(oracleConn);
            freeConnQueue.put(oracleConn);
            oracleConn.setStartSCN(0);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            releaseConnectionLock.unlock();
        }
    }
}