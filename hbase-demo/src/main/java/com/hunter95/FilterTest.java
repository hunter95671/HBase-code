package com.hunter95;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import java.io.IOException;
import java.util.List;

public class FilterTest {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void main(String[] args) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf("201906061515_emp"));

        Scan scan = new Scan();

        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator("40".getBytes()));
        scan.setFilter(rowFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for(Cell c: cells) {
                System.out.println(c);
            }
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

