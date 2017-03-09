package io.transwarp.hermes.core.sync;

import io.transwarp.hermes.common.model.HbaseModel;
import io.transwarp.hermes.common.model.SyncJobType;
import io.transwarp.hermes.core.hbase.HbaseContainer;
import io.transwarp.hermes.core.hbase.HbaseJob;
import io.transwarp.hermes.persistence.raw.HbaseConnection;
import io.transwarp.hermes.persistence.raw.HbaseConnectionPool;

/**
 * Created by rejudgex on 2/23/17.
 */

//负责去 put/upd/delete Container里面去取解析之后的hbase操作
//并调用HbaseJob同步到hbase中

public class HbaseManager implements Runnable{
    public SyncJobType jobType;
    HbaseConnection hbaseConnection = null;

    public HbaseManager(SyncJobType jobType) {
        setJobType(jobType);
        hbaseConnection = HbaseConnectionPool.getInstance().getConnection();
    }

    public void setJobType(SyncJobType jobType) {
        this.jobType = jobType;
    }

    public void run() {
        try {
            if (jobType.equals(SyncJobType.put)) {
                while (true) {
                    HbaseModel hbaseModel = HbaseContainer.getInstance().fetchHbaseFromPutQueue();
                    HbaseJob.insertRecord(hbaseConnection.getHbaseConnection(), hbaseModel);
                }
            }
            else if(jobType == SyncJobType.upd) {

            }
            else if(jobType == SyncJobType.delete) {

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            HbaseConnectionPool.getInstance().releaseConnection(hbaseConnection.getHbaseId());
        }
    }

    public void startHbaseSync(String jobType) {
        try {

        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void mergePut() {

    }

    public void mergeUpd() {

    }

    public static void main(String[] args) {
    }
}
