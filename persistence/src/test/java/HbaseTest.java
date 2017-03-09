import io.transwarp.hermes.persistence.raw.HbaseConnection;
import io.transwarp.hermes.persistence.raw.HbaseConnectionPool;
import io.transwarp.hermes.persistence.raw.HbaseUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Table;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by rejudgex on 2/27/17.
 */

public class HbaseTest{
    private static final String HBASE_URL = "HBASE_URL";
    private static final String HBASE_HOST = "HBASE_HOST";
    private static final String ZOOKEEPER_PORT = "ZOOKEEPER_PORT";

    private static HConnection hbaseConn = null;
    private static Configuration hbaseConf = null;
    private static Properties hbaseProps = null;

    private HbaseTest() throws Exception {
        InputStream is = HbaseConnection.class.getClassLoader().getResourceAsStream("config/secret.properties");
        hbaseProps = new Properties();
        hbaseProps.load(is);
    }

    static {
        try {
            hbaseConf = HBaseConfiguration.create();
            hbaseConf.set("hbase.master", hbaseProps.getProperty(HBASE_URL));
            hbaseConf.set("hbase.zookeeper.quorum", hbaseProps.getProperty(HBASE_HOST));
            hbaseConf.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        HbaseConnection hbaseConn = HbaseConnectionPool.getInstance().getConnection();
        try {
            TableName name = TableName.valueOf("htable1");
            Table table = hbaseConn.getHbaseConnection().getTable(name);
            System.out.println(table.getName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
