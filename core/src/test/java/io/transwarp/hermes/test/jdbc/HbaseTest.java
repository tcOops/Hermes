package io.transwarp.hermes.test.jdbc;


import io.transwarp.hermes.common.model.HbaseModel;
import io.transwarp.hermes.core.hbase.HbaseJob;
import io.transwarp.hermes.persistence.raw.HbaseConnection;
import io.transwarp.hermes.persistence.raw.HbaseConnectionPool;
import io.transwarp.hermes.persistence.raw.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HConnection;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by rejudgex on 2/27/17.
 */


public class HbaseTest implements Runnable {
    private static final String HBASE_URL = "HBASE_URL";
    private static final String HBASE_HOST = "HBASE_HOST";
    private static final String ZOOKEEPER_PORT = "ZOOKEEPER_PORT";
    private static ArrayBlockingQueue<HbaseModel> recordList;
    private static final String DEFAULT_COLUMN_FAMILY = "CLM";
    private static int RECORD_NUMBER = 100000;
    private static int HBASE_THREAD_NUMBER_1 = 40;
    private static int HBASE_THREAD_NUMBER_2 = 20;

    private static HConnection hbaseConn = null;
    private static Configuration hbaseConf = null;
    private static Properties hbaseProps = null;
    private HbaseConnection hbaseConnection;

    public HbaseTest() {}

    public void start() {
        recordList = new ArrayBlockingQueue<HbaseModel>(RECORD_NUMBER);
        HbaseModel hbaseModel = new HbaseModel();

        for(int i = 0; i < RECORD_NUMBER; ++i) {
            hbaseModel.setScn(123456);
            hbaseModel.setTableName("EMP");
            hbaseModel.setRowKey("124");
            List<String> colums = new ArrayList<String>();
            List<String> values = new ArrayList<String>();
            String[] clist = new String[]{"ENAME","JOB","MGR","HIREDATE","SAL","COMM","DEPTNO"};
            String[] vList = new String[]{"JONES","MANAGER","7839","TO_DATE('02-APR-81', 'DD-MON-RR')","2975", "NULL","20"};
            for(int j = 0; j < clist.length; ++j) {
                colums.add(clist[j]);
            }
            hbaseModel.setColumns(colums);
            for(int j = 0; j < vList.length; ++j) {
                values.add(vList[j]);
            }
            hbaseModel.setValues(values);
            recordList.add(hbaseModel);
        }
    }

    public void run() {
        try {
            hbaseConnection = HbaseConnectionPool.getInstance().getConnection();

            boolean suc = true;
            while(true) {
                List<HbaseModel> hbaseModels = new ArrayList<HbaseModel>();
                for (int i = 0; i < 3000; ++i) {
                    if (HbaseTest.recordList.size() == 0) {
                        suc = false;
                        System.out.println("Finish: " + System.currentTimeMillis());
                        break;
                    }
                    hbaseModels.add(HbaseTest.recordList.take());
                }
                HbaseJob.insertRecordList(hbaseConnection.getHbaseConnection(), hbaseModels);
                if(suc == false) break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //80s(1w条)
    @Test
    public void testSingleThread() {
        Connection connection = HbaseUtils.getConnection();
        start();
        long startTime = System.currentTimeMillis();
        while(true) {
            try {
                if(recordList.size() == 0) {
                    long endTime = System.currentTimeMillis();
                    System.out.println("1w put for single thread, cost : " + (endTime - startTime)*1.0/1000 + " s");
                    break;
                }
                HbaseModel hbaseModel = recordList.take();
                HbaseJob.insertRecord(connection, hbaseModel);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    //1000 in a group | 10w条： 1000个合并要20.453s,
    @Test
    public void testSingleThreadGroup() {
        Connection connection = HbaseUtils.getConnection();
        start();
        long startTime = System.currentTimeMillis();
        int cnt = 0;
        while(true) {
            System.out.println(cnt);
            try {
                if(recordList.size() == 0) {
                    long endTime = System.currentTimeMillis();
                    System.out.println("1w put for single thread, cost : " + (endTime - startTime)*1.0/1000 + " s");
                    break;
                }

                List<HbaseModel> hbaseModels = new ArrayList<HbaseModel>();
                for(int i = 0; i < 1000; ++i) {
                    hbaseModels.add(recordList.take());
                }
                HbaseJob.insertRecordList(connection, hbaseModels);
            } catch (Exception e) {
                e.printStackTrace();
            }
            ++cnt;
        }
    }


    @Test
    public void testMultiThreadGroup() {
        start();
        ExecutorService hbasePool = Executors.newFixedThreadPool(40);
        for(int i = 0; i < 40; ++i) {
            hbasePool.execute(new HbaseTest());
        }
    }

    //40个线程不算启动时间 2.4s(1w数据, 10w: 1000合并的话要15-20s, 3000条合并写的话0.1s以内
    public static void main(String[] args) {
        HbaseTest hbaseTest = new HbaseTest();
        hbaseTest.start();
        ExecutorService hbasePool = Executors.newFixedThreadPool(40);
        for(int i = 0; i < 40; ++i) {
            hbasePool.execute(new HbaseTest());
        }
    }
}
