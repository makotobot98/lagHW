package com.makoto.zk;

import java.sql.Connection;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.commons.dbcp2.BasicDataSource;
import java.sql.SQLException;
import org.I0Itec.zkclient.ZkClient;


public class DBUtil {
    private static BasicDataSource ds;

    public static String connURL = "jdbc:mysql://linux123:3306";
    private static String passwordStr = "12345678";
    private static  String userNameStr = "root";
    private static String zkConnStr = "linux123:2181";
    private static ZkClient zkClient;

    public static void initDataSource()
    {

        if (ds == null)
        {
            ds = new BasicDataSource();
            ds.setUrl(connURL);
            ds.setUsername(userNameStr);
            ds.setPassword(passwordStr);
            ds.setMinIdle(5);
            ds.setMaxIdle(10);
        }

        //initialize zkclient
        if (zkClient == null) {
            zkClient = new ZkClient(zkConnStr);
            //initialize serializer for zkclient
            zkClient.setZkSerializer(new ZKStrSerializer());

            //create sqlConn dir if not exist already
            final boolean exists = zkClient.exists("/hw3-sqlConn");

            if (!exists) {
                zkClient.createPersistent("/hw3-sqlConn", connURL);
            }

            //subscribing for connection changes
            zkClient.subscribeDataChanges("/hw3-sqlConn", new IZkDataListener() {
                @Override
                public void handleDataChange(String s, Object o) throws Exception {
                    String newConnURL = String.valueOf(o);

                    //create a new pool upon information changes
                    ds = createConnPool(newConnURL);
                }

                @Override
                public void handleDataDeleted(String s) throws Exception {

                }
            });
        }

    }

    public static Connection getDBConnection() throws SQLException {
        return ds.getConnection();
    }

    private static BasicDataSource createConnPool(String url) {
        connURL = url;
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(connURL);
        dataSource.setUsername(userNameStr);
        dataSource.setPassword(passwordStr);
        dataSource.setMinIdle(5);
        dataSource.setMaxIdle(10);
        return dataSource;
    }


}
