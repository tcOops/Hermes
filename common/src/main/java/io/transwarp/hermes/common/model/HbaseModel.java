package io.transwarp.hermes.common.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rejudgex on 3/7/17.
 */
public class HbaseModel implements Cloneable{
    private long scn;
    private String tableName;
    private String rowKey;
    private String columnFamily = "CLK";
    private List<String> columns;
    private List<String> values;

    public HbaseModel() {

    }

    public HbaseModel(long scn, String tableName, String rowKey, String columnFamily, ArrayList<String> columns, ArrayList<String> values) {
        this.scn = scn;
        this.tableName = tableName;
        this.rowKey = rowKey;
        this.columnFamily = columnFamily;
        this.columns = columns;
        this.values = values;
    }

    public String getTableName() {
        return tableName;
    }

    public long getScn() {
        return scn;
    }

    public void setScn(long scn) {
        this.scn = scn;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getColumnFamily() { return columnFamily; }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<String> getValues() { return values; }

    public void setValues(List<String> values) { this.values = values; }

    public HbaseModel clone() {
        HbaseModel cloned = null;
        try {
            cloned = (HbaseModel) super.clone();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return cloned;
    }
}
