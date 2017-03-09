package io.transwarp.hermes.test.jdbc;

/**
 * Created by rejudgex on 2/24/17.
 */
import io.transwarp.hermes.persistence.raw.OracleConnection;
import org.junit.Test;

import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class CreateOracleData {
    private Connection conn;
    private Statement statement;
    private ResultSet resultSet;
    private static final String ORACLE_JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private Properties secretProps;

    public void startConn() throws Exception{
        Class.forName(ORACLE_JDBC_DRIVER);
        InputStream is = OracleConnection.class.getClassLoader().getResourceAsStream("config/secret.properties");
        secretProps = new Properties(

        );
        secretProps.load(is);
        conn = DriverManager.getConnection(secretProps.getProperty("ORACLE_URL"), secretProps.getProperty("ORACLE_USERNAME"), secretProps.getProperty("ORACLE_PASSWORD"));

        statement = conn.createStatement();
    }

    @Test
    public void insertRow() throws Exception{
        try {
            startConn();

            String querySQL = "select EMPNO from EMP where rownum <= 1 order by EMPNO desc";
            resultSet = statement.executeQuery(querySQL);
            long beginId = -1;
            while(resultSet.next()) {
                beginId = Long.parseLong(resultSet.getString("EMPNO"));
                System.out.println(beginId);
                break;
            }

            for(int i = 1; i <= 1000; ++i) {
                conn.setAutoCommit(false);
                String insertSQL = String.format("INSERT INTO EMP VALUES (%d,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,20)", beginId + i);
                int statusCode = statement.executeUpdate(insertSQL);
                if(statusCode < 0) {
                    conn.rollback();
                    statement.close();
                    conn.close();
                }
                else {
                    conn.commit();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            conn.close();
        }
    }
}
