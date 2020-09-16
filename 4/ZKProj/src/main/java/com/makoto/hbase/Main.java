package com.makoto.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Test;
import org.junit.Before;

import java.io.IOException;

public class Main {
    private static final String TABLE_NAME = "user_relation";
    private static final String FAMAILLY_NAME = "friends";
    Configuration conf=null;
    Connection conn=null;
    HBaseAdmin admin =null;

    @Before
    public void init () throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","linux121,linux122");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conn = ConnectionFactory.createConnection(conf);
    }

    public void destroy(){
        if(admin!=null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(conn !=null){
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void createTable() throws IOException {
        admin = (HBaseAdmin) conn.getAdmin();
        HTableDescriptor tb = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        tb.addFamily(new HColumnDescriptor(FAMAILLY_NAME));
        admin.createTable(tb);
    }


}
