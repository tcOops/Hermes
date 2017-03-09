package io.transwarp.hermes.persistence.raw;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.CallableStatement;
import java.sql.Statement;
import java.util.Properties;

import jdk.nashorn.internal.codegen.CompilerConstants;
import org.junit.Test;

/**
 * Created by rejudgex on 2/22/17.
 */
public class OracleConnection {
    private Connection conn;
    private Statement statement;
    private CallableStatement callableStatement;
    private int startSCN;
    Properties secretProps;

    public OracleConnection(Connection conn) {
        try {
            InputStream is = OracleConnection.class.getClassLoader().getResourceAsStream("config/secret.properties");
            secretProps = new Properties();
            secretProps.load(is);

            String redoLogDictionary = secretProps.getProperty("REDO_LOG_DICTIONARY_FILE_PATH");
            System.out.println(redoLogDictionary);
            String createDictSql = "BEGIN dbms_logmnr_d.build(dictionary_filename => 'dictionary.ora', dictionary_location =>'"
                    + redoLogDictionary + "'); END;";
            this.conn = conn;
            this.callableStatement = conn.prepareCall(createDictSql);
            this.statement = conn.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
       /* try {
            InputStream is = OracleConnection.class.getClassLoader().getResourceAsStream("config/secret.properties");
            Properties secretProps = new Properties();
            secretProps.load(is);

            String redoLogDictionary = secretProps.getProperty("REDO_LOG_DICTIONARY_FILE_PATH");
            System.out.println(redoLogDictionary);
        } catch (Exception e) {

        }*/
    }

    @Override
    protected void finalize() throws Throwable {
        conn.close();
        callableStatement.close();
    }

    public Connection getConnection() {
        return conn;
    }

    public Statement getStatement() {
        return statement;
    }

    public CallableStatement getCallableStatement() {
        return callableStatement;
    }

    public int getStartSCN() {
        return startSCN;
    }

    public void setStartSCN(int startSCN) {
        this.startSCN = startSCN;
    }
}
