package com.hunter95;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class ExampleForHBase3 {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;


    public static void main(String[] args)throws IOException{

        init();

        String tableName = "counters";
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));

            long count1 = table.incrementColumnValue(Bytes.toBytes("20150101"),Bytes.toBytes("daily"),Bytes.toBytes("hits"),1);
            long count2 = table.incrementColumnValue(Bytes.toBytes("20150101"),Bytes.toBytes("daily"),Bytes.toBytes("hits"),1);
            long currentCount = table.incrementColumnValue(Bytes.toBytes("20150101"),Bytes.toBytes("daily"),Bytes.toBytes("hits"),0);
            System.out.println("count1: "+count1);
            System.out.println("count2: "+count2);
            System.out.println("currentCount: "+currentCount);
        } catch (IOException e) {
            e.printStackTrace();
        }

        close();
    }

    public static void init(){
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void close(){
        try{
            if(admin != null){
                admin.close();
            }
            if(null != connection){
                connection.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}


