package io.transwarp.hermes.core.oracle;

import org.junit.Test;

import java.sql.SQLException;

/**
 * Created by rejudgex on 2/23/17.
 */

public class SQLExtractManager implements Runnable{
    public long activeLogId;
    public long redoLogStartSCN;
    public long startSCN;
    public long endSCN;

    public SQLExtractManager() {
        startSCN = -1;
        activeLogId = -1;
    }

    @Test
    public void run() {
        SQLScan sqlScan = new SQLScan();

        while(true) {
            long[] dbStatus = sqlScan.getDBStatus();
            if(activeLogId == -1) {//首次加载log
                startSCN = dbStatus[1];
                activeLogId = dbStatus[0];
                endSCN = sqlScan.getCurrentSCN();
                sqlScan.loadRedoLog(dbStatus[0]);
                sqlScan.createView(dbStatus[1]);
                sqlScan.startAnalysis(startSCN, endSCN);
            //    System.out.print("type 1:  ");
            }
            else if(dbStatus[0] == activeLogId) {//非首次加载log，且log没有切换
                endSCN = sqlScan.getCurrentSCN();
                sqlScan.startAnalysis(startSCN, endSCN);
            //    System.out.print("type 2:  ");
            }
            else {//log发生切换， 解决旧log的结尾scn以及新log的scn
                endSCN = sqlScan.getCurrentSCN();
                sqlScan.startAnalysis(startSCN, endSCN);
                activeLogId = dbStatus[0];
                sqlScan.loadRedoLog(activeLogId);
                sqlScan.createView(dbStatus[1]);
                sqlScan.startAnalysis(dbStatus[1], endSCN);
            //    System.out.print("type 3:  ");
            }
            startSCN = endSCN + 1;
        }
    }

    public long getStartSCN() {
        return startSCN;
    }
}
