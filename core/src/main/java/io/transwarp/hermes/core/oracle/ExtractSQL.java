package io.transwarp.hermes.core.oracle;

/**
 * Created by rejudgex on 2/19/17.
 */
import java.sql.*;
import io.transwarp.hermes.common.common.Constants;

public class ExtractSQL {
    public void createDict(Connection oracleConnection) throws Exception {
        String createDictSql = "BEGIN dbms_logmnr_d.build(dictionary_filename => 'dictionary.ora', dictionary_location =>'"
                + Constants.REDO_LOG_DICTIONARY_FILE_PATH + "'); END;";
        System.out.println(createDictSql);
        CallableStatement callableStatement = oracleConnection.prepareCall(createDictSql);
        callableStatement.execute();
    }

    public void startLogMiner() throws Exception {
        Connection oracleConn = null;
        try {
            ResultSet resultSet = null;
            CallableStatement callableStatement = null;
            Class.forName("oracle.jdbc.driver.OracleDriver");
            oracleConn = DriverManager.getConnection(Constants.ORACLE_URL, Constants.ORACLE_USERNAME, Constants.ORACLE_PASSWORD);
            Statement statement = oracleConn.createStatement();

            //Check which redo_log is active now
            String findActiveLog = "SELECT group#, status FROM V$log";
            resultSet = statement.executeQuery(findActiveLog);
            String activeLogId = null;
            while(resultSet.next()) {
                String redoLogStatus = resultSet.getString("status").toString();
                if(redoLogStatus.equals("CURRENT")) {
                    activeLogId = resultSet.getString("group#");
                    break;
                }
            }


            createDict(oracleConn);
            StringBuilder sb = new StringBuilder();
            sb.append("BEGIN");
            sb.append(" dbms_logmnr.add_logfile(logfilename=>'" + Constants.REDO_LOG_PATH
                    + "/redo0" + activeLogId + ".log', options=>dbms_logmnr.NEW);");
           //         + "/redo01.log', options=>dbms_logmnr.NEW);");
            sb.append(" dbms_logmnr.add_logfile(logfilename=>'" + Constants.REDO_LOG_PATH
                    + "/redo02.log', options=>dbms_logmnr.NEW);");
            //sb.append(" dbms_logmnr.add_logfile(logfilename=>'" + Constants.REDO_LOG_PATH
             //       + "/redo03.log', options=>dbms_logmnr.ADDFILE);");

            sb.append(" END;");
            System.out.println(sb);
            callableStatement = oracleConn.prepareCall(sb + " ");
            callableStatement.execute();

            String logQuery = "select db_name, thread_sqn, filename from v$logmnr_logs";
            resultSet = statement.executeQuery(logQuery);
            while(resultSet.next()) {
                System.out.println("Add redo log file Already ===> " + resultSet.getObject(3));
            }

            System.out.println("Start Logminer now with Start SCN: " + Constants.START_SCN);
            String startLogMinerSQL = "BEGIN dbms_logmnr.start_logmnr(startScn=>'" + Constants.START_SCN
                    + "',dictfilename=>'" + Constants.REDO_LOG_DICTIONARY_FILE_PATH
                    + "/dictionary.ora');END;";
            System.out.println(startLogMinerSQL);
            callableStatement = oracleConn.prepareCall(startLogMinerSQL);
            callableStatement.execute();
            System.out.println("Complete Logminer Analysis");

            System.out.println("Get Analysis Result");
            String resultSQL = "select scn, sql_redo from v$logmnr_contents where" +
                    " seg_owner = '" + Constants.ORACLE_USERNAME + "' and seg_name = 'EMP'";
            resultSet = statement.executeQuery(resultSQL.toString());
            while(resultSet.next()) {
                System.out.println(resultSet.getString(1).toString() + " " + resultSet.getString(2).toString());
            }
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            if(null != oracleConn) {
                oracleConn = null;
            }
        }
    }

    public static void main(String[] args) {
        try {
            ExtractSQL extractSQL = new ExtractSQL();
            extractSQL.startLogMiner();
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }
}
