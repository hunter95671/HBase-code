package com.hunter95;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import java.io.IOException;

public class ExampleForHBase {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void main(String[] args)throws IOException{

        insertRow("201906061515_emp", "50", "201906061515_info1", "201906061515_ename", "zhangsan");
        insertRow("201906061515_emp", "51", "201906061515_info1", "201906061515_ename", "lisi");
        insertRow("201906061515_emp", "52", "201906061515_info1", "201906061515_ename", "wangwu");

        System.out.println("--------------------------------");
        getData("201906061515_emp", "50", "201906061515_info1", "201906061515_ename");
        getData("201906061515_emp", "40", "201906061515_info1", "");

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

    public static void insertRow(String tableName,String rowKey,String colFamily,String col,String val) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
        table.put(put);
        System.out.println("insert row success");
        table.close();
        close();
    }

    public static void getData(String tableName,String rowKey,String colFamily,String col)throws  IOException{
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(colFamily.getBytes(),col.getBytes());
        Result result = table.get(get);
        showCell(result);
        table.close();
        close();
    }

    public static void showCell(Result result){
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
            System.out.println("Timetamp:"+cell.getTimestamp()+" ");
            System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
            System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
            System.out.println("--------------------------------");
        }
    }
}
