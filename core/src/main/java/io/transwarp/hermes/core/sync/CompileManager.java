package io.transwarp.hermes.core.sync;

import io.transwarp.hermes.common.model.HbaseModel;
import io.transwarp.hermes.core.hbase.HbaseContainer;
import io.transwarp.hermes.core.oracle.SQLContainer;
import io.transwarp.hermes.core.oracle.SQLExtractManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rejudgex on 2/23/17.
 */

//负责将SqlContainer中的SQL进行解析
//将解析结果放到 put/upd/delete 任务队列中
//同时启动HbaseManage多线程任务来执行， 构成生产者消费者

public class CompileManager implements Runnable{
    private static final String DEFAULT_COLUMN_FAMILY = "CLM";

    public CompileManager() {

    }

    public void run() {
        while(true) {
            String sqlContent = SQLContainer.getInstance().fetchSQLFromContainer();
          //  System.out.println(sqlContent);
            sqlCompileCore(sqlContent);
        }
    }

    public void sqlCompileCore(String sqlContent) {
     //   System.out.println("curGet:  " + sqlContent);
        String[] contents = sqlContent.split("&&&");
        long scn = Long.parseLong(contents[0]);
        String operation = contents[1].split(" ")[0];
        String regEx = "\"";
        contents[1] = contents[1].replaceAll(regEx, "");
        regEx = "\'";
        contents[1] = contents[1].replaceAll(regEx, "");

        if(operation.equals("insert")) {
            HbaseModel hbaseModel = generatePutHbaseModel(contents[1], scn);
            try {
                HbaseContainer.getInstance().push2PutQueue(hbaseModel);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
        else if(operation.equals("update")) {

        }
        else if(operation.equals("delete")) {

        }
    }

    public HbaseModel generatePutHbaseModel(String sqlContent, long scn) {
        String regEx = "\\.(.*?)\\((.*?)\\)"; //正则非贪婪匹配 (.*?)这里加个问号
        Pattern pattern = Pattern.compile(regEx);
        Matcher matcher = pattern.matcher(sqlContent);
        matcher.find();
        String tableName = matcher.group(1);
        String[] columnList = matcher.group(2).split(",");
        ArrayList<String> columns = new ArrayList<String>();
        for(int i = 1; i < columnList.length; ++i) {
            columns.add(columnList[i]);
        }

        regEx = "values.?\\((.*)\\)"; //values非贪婪匹配
        pattern = Pattern.compile(regEx);
        matcher = pattern.matcher(sqlContent);
        matcher.find();
        String[] valueBeforeFormat = matcher.group(1).split(",");

        ArrayList<String> values = new ArrayList<String>();
        String rowKey = valueBeforeFormat[0];
        for(int i = 1; i < valueBeforeFormat.length; ) {
            if(valueBeforeFormat[i].length() >= 7 && valueBeforeFormat[i].substring(0, 7).equals("TO_DATE")) {
                values.add(valueBeforeFormat[i] + valueBeforeFormat[i+1]);
                i += 2;
            }
            else {
                values.add(valueBeforeFormat[i]);
                ++i;
            }
        }
        HbaseModel hbaseModel = new HbaseModel(scn, tableName, rowKey, DEFAULT_COLUMN_FAMILY, columns, values);
        return hbaseModel;
    }

    public static void main(String[] args) {
        // SyncManager syncManager = new SyncManager();
        //  syncManager.startSync();
      /*  String sqlContent = "2560474&&&insert into \"LOGMINER\".\"EMP\"(\"EMPNO\",\"ENAME\",\"JOB\",\"MGR\",\"HIREDATE\",\"SAL\",\"COMM\",\"DEPTNO\") values ('12004','JONES','MANAGER','7839',TO_DATE('02-APR-81', 'DD-MON-RR'),'2975',NULL,'20');";

        String[] contents = sqlContent.split("&&&");
        long scn = Long.parseLong(contents[0]);
        contents = contents[1].split(" ");
        String operation = contents[0];
        for(int i = 0; i < contents.length; ++i) {
            System.out.println(contents[i]);
        }
        String tableName = contents[2].split("\\(")[0].split("\\.")[1];*/
      //  String[] columns = contents[2].split("\\(")[1].split("\\)")[0].split(",");
      //  System.out.println(tableName);
        String str = "insert into \"LOGMINER\".\"EMP\"(\"EMPNO\",\"ENAME\",\"JOB\",\"MGR\",\"HIREDATE\",\"SAL\",\"COMM\",\"DEPTNO\") values ('12022','JONES','MANAGER','7839',TO_DATE('02-APR-81', 'DD-MON-RR'),'2975',NULL,'20');";
        str = "TO_DATE(02-APR-81";
        System.out.println(str.substring(0, 8));
    }
}
