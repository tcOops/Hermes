package io.transwarp.hermes.core.oracle;

import io.transwarp.hermes.persistence.raw.OracleConnection;
import io.transwarp.hermes.persistence.raw.OracleConnectionPool;

import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.Connection;
import java.util.Properties;

/**
 * Created by rejudgex on 2/22/17.
 */
public class SQLScan {
    private static final String ORACLE_USERNAME = "ORACLE_USERNAME";
    private static final String START_SCN = "START_SCN";
    private static final String REDO_LOG_PATH = "REDO_LOG_PATH";
    private static final String REDO_LOG_DICTIONARY_FILE_PATH = "REDO_LOG_DICTIONARY_FILE_PATH";

    private OracleConnection oracleConn = null;
    private Statement statement;
    private ResultSet resultSet;
    private CallableStatement callableStatement;
    private Properties secretProps;

    public SQLScan() {
        try {
            oracleConn = OracleConnectionPool.getInstance().getConnection();
            callableStatement = oracleConn.getCallableStatement();
            statement = oracleConn.getStatement();
            InputStream is = OracleConnection.class.getClassLoader().getResourceAsStream("config/secret.properties");
            secretProps = new Properties();
            secretProps.load(is);

            oracleConn.getCallableStatement().execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public long[] getDBStatus() {
        long[] dbStatus = new long[2];
        try {
            String findActiveLog = "SELECT group#, status, first_change# FROM V$log";
            resultSet = statement.executeQuery(findActiveLog);
            while (resultSet.next()) {
                String redoLogStatus = resultSet.getString("status").toString();
                if (redoLogStatus.equals("CURRENT")) {
                    long activeLogId = Integer.parseInt(resultSet.getString("group#"));
                    long startSCN = Long.parseLong(resultSet.getString("first_change#").toString());
                    dbStatus[0] = activeLogId;
                    dbStatus[1] = startSCN;
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return dbStatus;
        }
    }

    public void loadRedoLog(long activeLogId) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("BEGIN");
            sb.append(" dbms_logmnr.add_logfile(logfilename=>'" + secretProps.getProperty(REDO_LOG_PATH)
                    + "/redo0" + activeLogId + ".log', options=>dbms_logmnr.NEW);");
            sb.append(" END;");
            System.out.println(sb);
            callableStatement = oracleConn.getConnection().prepareCall(sb + " ");
            callableStatement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createView(long redoLogStartSCN) {
        try {
            System.out.println("Start Logminer now with Start SCN: " + String.valueOf(redoLogStartSCN));
            String startLogMinerSQL = "BEGIN dbms_logmnr.start_logmnr(startScn=>'" + String.valueOf(redoLogStartSCN)
                    + "',dictfilename=>'" + secretProps.getProperty(REDO_LOG_DICTIONARY_FILE_PATH)
                    + "/dictionary.ora');END;";
            callableStatement = oracleConn.getConnection().prepareCall(startLogMinerSQL);
            callableStatement.execute();
            System.out.println("Complete Logminer Analysis");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public long getCurrentSCN() {
        long currentSCN = -1;
        try {
            String findCurrentSCN = "select current_scn from v$database";
            resultSet = statement.executeQuery(findCurrentSCN);
            while (resultSet.next()) {
                currentSCN = Long.parseLong(resultSet.getString("current_scn"));
                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return currentSCN;
    }

    public void startAnalysis(long startSCN, long endSCN) {
        try {
            System.out.println("Get Analysis Result");
            String resultSQL = "select scn, sql_redo from v$logmnr_contents where" +
                    " seg_owner = '" + secretProps.getProperty(ORACLE_USERNAME) + "' and seg_name = 'EMP'"
                    + "and scn >= " + String.valueOf(startSCN) + " and scn <= " + String.valueOf(endSCN);
            resultSet = statement.executeQuery(resultSQL.toString());
            while (resultSet.next()) {
                StringBuilder sqlContent = new StringBuilder();
                sqlContent.append(resultSet.getString("scn").toString());
                sqlContent.append("&&&");
                sqlContent.append(resultSet.getString("sql_redo").toString());

                SQLContainer.getInstance().pushSQL2Container(sqlContent.toString());
             //   System.out.println(sqlContent.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void releaseOracleConnection() {
        OracleConnectionPool.getInstance().releaseConnection(oracleConn);
    }

    public static void main(String[] args) {
        SQLScan sqlScan = new SQLScan();
        long[] dbStatus = sqlScan.getDBStatus();
        sqlScan.loadRedoLog(dbStatus[0]);
        sqlScan.createView(dbStatus[1]);
    }
}
