package io.transwarp.hermes.core.hbase;

import io.transwarp.hermes.common.model.HbaseModel;
import io.transwarp.hermes.persistence.raw.HbaseConnection;
import io.transwarp.hermes.persistence.raw.HbaseConnectionPool;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created by rejudgex on 3/6/17.
 */
public class HbaseJob {
    public static void createTable(Connection connection, String tableName, String[] columns) throws IOException {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            TableName name = TableName.valueOf(tableName);
            if(admin.tableExists(name)) {
                admin.disableTable(name);
                admin.deleteTable(name);
            }
            else {
                HTableDescriptor desc = new HTableDescriptor();
                desc.setName(TableName.valueOf(tableName));
                for(String column : columns) {
                    desc.addFamily(new HColumnDescriptor(column));
                }
                admin.createTable(desc);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            admin.close();
        }
    }

    public static void insertRecord (Connection connection, HbaseModel hbaseModel) throws Exception{
        Table table = null;
        String tableName = hbaseModel.getTableName();
        tableName = tableName.substring(1, tableName.length() - 1);
        TableName name = TableName.valueOf(tableName);
        table = connection.getTable(name);
        Put put = new Put(Bytes.toBytes(hbaseModel.getRowKey()));

        List<String> columns = hbaseModel.getColumns();
        List<String> values = hbaseModel.getValues();
        int len = columns.size();
        for(int i = 0; i < len; ++i) {
            put.addColumn(Bytes.toBytes(hbaseModel.getColumnFamily()),
                    Bytes.toBytes(String.valueOf(columns.get(i))),
                    Bytes.toBytes(values.get(i)));
            table.put(put);
        }
        System.out.println("Put record to Hbase Completed  " + hbaseModel.getScn());
    }


    public static void main(String[] args) {
        HbaseConnection connection = HbaseConnectionPool.getInstance().getConnection();
        String[] CLM = new String[]{"CLM"};
        try {
            createTable(connection.getHbaseConnection(), "EMP", CLM);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
