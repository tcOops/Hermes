package io.transwarp.hermes.persistence.raw;

/**
 * Created by rejudgex on 3/1/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Connection;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HbaseUtils {
    private static final String HBASE_URL = "HBASE_URL";
    private static final String HBASE_HOST = "HBASE_HOST";
    private static final String ZOOKEEPER_PORT = "ZOOKEEPER_PORT";
    private static Properties hbaseProps = null;

    private static Configuration hbaseConf = HBaseConfiguration.create();
    private static ExecutorService poolx = Executors.newFixedThreadPool(300);

    public static Connection getConnection(){
        InputStream is = OracleConnection.class.getClassLoader().getResourceAsStream("config/secret.properties");
        hbaseProps = new Properties();

        int i = 0;
        Connection conn = null;
        do{
            try {
                hbaseProps.load(is);
                hbaseConf.set("hbase.master", hbaseProps.getProperty(HBASE_URL));
                hbaseConf.set("hbase.zookeeper.quorum", hbaseProps.getProperty(HBASE_HOST));
                hbaseConf.set("hbase.zookeeper.property.clientPort", hbaseProps.getProperty(ZOOKEEPER_PORT));
                conn = ConnectionFactory.createConnection(hbaseConf, poolx);
                if(conn != null){
                    break;
                }
                Thread.sleep(100);
                i++;
            } catch(InterruptedException e){
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } while(conn == null && i < 5);
        return conn;
    }

    public static void closeConnection(Connection connection) {
        try {
            connection.close();
            poolx.shutdownNow();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
