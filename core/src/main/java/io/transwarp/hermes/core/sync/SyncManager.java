package io.transwarp.hermes.core.sync;

import io.transwarp.hermes.common.model.HbaseModel;
import io.transwarp.hermes.common.model.SyncJobType;
import io.transwarp.hermes.core.hbase.HbaseContainer;
import io.transwarp.hermes.core.oracle.SQLContainer;
import io.transwarp.hermes.core.oracle.SQLExtractManager;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by rejudgex on 2/23/17.
 */

//负责将SqlContainer中的SQL进行解析
//将解析结果放到 put/upd/delete 任务队列中
//同时启动HbaseManage多线程任务来执行， 构成生产者消费者

public class SyncManager{
    private static final int COMPILE_THREAD_NUMBER = 10;
    private static final int HBASE_THREAD_NUMBER = 40;

    public SyncManager() {

    }

    @Test
    public void startSync() throws Exception{
        new Thread(new SQLExtractManager()).start();

        ExecutorService compileThreadPool = Executors.newFixedThreadPool(COMPILE_THREAD_NUMBER + 1);
        for(int i = 0; i < COMPILE_THREAD_NUMBER; ++i) {
            compileThreadPool.execute(new CompileManager());
        }

        ExecutorService hbaseThreadPool = Executors.newFixedThreadPool(HBASE_THREAD_NUMBER);
        for(int i = 0; i < HBASE_THREAD_NUMBER/3; ++i) {
            hbaseThreadPool.execute(new HbaseManager(SyncJobType.put));
            hbaseThreadPool.execute(new HbaseManager(SyncJobType.upd));
            hbaseThreadPool.execute(new HbaseManager(SyncJobType.delete));
        }
    }

    public static void main(String[] args) {
        try {
            SyncManager syncManager = new SyncManager();
            syncManager.startSync();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
