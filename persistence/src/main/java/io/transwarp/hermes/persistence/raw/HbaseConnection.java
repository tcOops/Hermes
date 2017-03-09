package io.transwarp.hermes.persistence.raw;

import org.apache.hadoop.hbase.client.Connection;
import io.transwarp.hermes.common.model.HbaseStatus;

/**
 * Created by rejudgex on 3/1/17.
 */
public class HbaseConnection {
    private String hbaseId;
    private Connection hbaseConn;
    private HbaseStatus hbaseStatus;

    public Connection getHbaseConnection() {
        return hbaseConn;
    }

    public void setHbaseConnection(Connection connection) {
        this.hbaseConn = connection;
    }

    public HbaseStatus getHbaseStatus() {
        return hbaseStatus;
    }

    public void setHbaseStatus(HbaseStatus status) {
        this.hbaseStatus = status;
    }

    public String getHbaseId() {
        return hbaseId;
    }

    public void setHbaseId(String id) {
        this.hbaseId = id;
    }
}
