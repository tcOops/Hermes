package io.transwarp.hermes.persistence.raw;

/**
 * Created by rejudgex on 2/15/17.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class ConnectionFactory {
    public static Configuration configuration;
    public static final String host = "127.0.0.1";

    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", host + ":60000");
        configuration.set("hbase.zookeeper.quorum", host);
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public static void createTable(String tableName, String[] family) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(configuration);
        if(admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Delete Table Now");
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        for(String each:family) {
            tableDescriptor.addFamily(new HColumnDescriptor(each));
        }
        admin.createTable(tableDescriptor);
    }

    public static void insertTable(String tableName, String rowKey, String[] column1, String[] value1, String[] column2, String[] value2) throws Exception{
        Put put = new Put(Bytes.toBytes(rowKey));
        HTable table = new HTable(configuration, Bytes.toBytes(tableName));
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();

        for(HColumnDescriptor each : columnFamilies) {
            String familyName = each.getNameAsString();
            System.out.println(familyName);
            if(familyName.equals("article")) {
                for(int j = 0; j < column1.length; ++j) {
                    put.add(Bytes.toBytes(familyName), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
            if(familyName.equals("author")) {
                for(int j = 0; j < column2.length; ++j) {
                    put.add(Bytes.toBytes(familyName), Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                }
            }
        }

        table.put(put);
    }

    public static void main(String [] args) {
        String tableName = "htable1", rowKey = "row1";

        String[] column1 = { "title", "content", "tag" };
        String[] value1 = {
                "Head First HBase",
                "HBase is the Hadoop database. Use it when you need random, realtime read/write access to your Big Data.",
                "Hadoop,HBase,NoSQL" };
        String[] column2 = { "name", "nickname" };
        String[] value2 = { "nicholas", "lee" };

        try {
            String[] columnFamilies = new String[]{"author", "article"};
            createTable(tableName, columnFamilies);

            insertTable(tableName, rowKey, column1, value1, column2, value2);
        }catch (Exception e) {
            System.out.println(e);
        }
    }
}
