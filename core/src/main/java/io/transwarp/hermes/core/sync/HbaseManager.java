package io.transwarp.hermes.core.sync;

import io.transwarp.hermes.common.model.HbaseModel;
import io.transwarp.hermes.common.model.SyncJobType;
import io.transwarp.hermes.core.hbase.HbaseContainer;
import io.transwarp.hermes.core.hbase.HbaseJob;
import io.transwarp.hermes.persistence.raw.HbaseConnection;
import io.transwarp.hermes.persistence.raw.HbaseConnectionPool;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rejudgex on 2/23/17.
 */

//负责去 put/upd/delete Container里面去取解析之后的hbase操作
//并调用HbaseJob同步到hbase中

public class HbaseManager implements Runnable{
    private static final int UPD_UPPER_COUNT_ONE_GROUP = 3000;
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
                    List<HbaseModel> hbaseModelList = new ArrayList<HbaseModel>();
                    int hbaseModelCount = 0;
                    while(true) {
                        if(HbaseContainer.getInstance().getPutQueueSize() == 0) {
                            break;
                        }
                        HbaseModel hbaseModel = HbaseContainer.getInstance().fetchHbaseFromPutQueue();
                        hbaseModelList.add(hbaseModel);
                        ++hbaseModelCount;
                        if(hbaseModelCount == UPD_UPPER_COUNT_ONE_GROUP) {
                            break;
                        }
                    }
                    if(hbaseModelList.size() != 0) {
                        System.out.println("Group Number : " + hbaseModelCount);
                        HbaseJob.insertRecordList(hbaseConnection.getHbaseConnection(), hbaseModelList);
                    }
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
