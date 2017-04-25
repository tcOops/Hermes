package io.transwarp.hermes.core.oracle;

/**
 * Created by rejudgex on 2/23/17.
 */
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SQLContainer {
    private static SQLContainer sqlContainer = null;
    private static final int SQL_CONTAINER_MAX_SIZE = 5000;
    private BlockingQueue<String> sqlQueue;

    private SQLContainer() {
        sqlQueue = new LinkedBlockingQueue<String>(SQL_CONTAINER_MAX_SIZE);
    }

    public static synchronized SQLContainer getInstance() {
        if(sqlContainer == null) {
            sqlContainer = new SQLContainer();
        }
        return sqlContainer;
    }

    public void pushSQL2Container(String rawSQL) {
        try {
            sqlQueue.put(rawSQL);
       //     System.out.println("put " + sqlQueue.size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String fetchSQLFromContainer() {
        try {
        //    System.out.println("take " + sqlQueue.size());
            return sqlQueue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
}

