package io.transwarp.hermes.common.common;

/**
 * Created by rejudgex on 2/16/17.
 */

public class Constants {
    public static final int HBASE_CONN_COUNT = 10;
    public static final int ORACLE_CONN_COUNT = 3;
    public static final String ORACLE_DRIVER = "oracle.jdbc.drive.OracleDriver";
    public static final String ORACLE_URL = "jdbc:oracle:thin:@127.0.0.1:1521:orcl";
    public static final String ORACLE_USERNAME = "LOGMINER";
    public static final String ORACLE_PASSWORD = "LOGMINER";
    public static final String START_SCN = "2070367";
    public static final String REDO_LOG_DICTIONARY_FILE_PATH = "/opt/oracle/oradata/orcl/LOGMNR";
    public static final String REDO_LOG_PATH = "/opt/oracle/oradata/orcl";
}
