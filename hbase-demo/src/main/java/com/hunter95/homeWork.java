package com.hunter95;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
public class homeWork {

    private static Connection connection=null;
    private static Admin admin=null;
    static {
        try {
            //1.获取配置信息
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

            //2.创建连接对象
            connection=ConnectionFactory.createConnection(configuration);

            //3.创建admin对象
            admin=connection.getAdmin();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //向表插入数据
    public static void putData(String tableName,String rowKey,String cf,String cn,String value) throws IOException {

        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //3.给put对象赋值
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));

        //4.插入数据
        table.put(put);

        //5.关闭连接
        table.close();

    }

    //关闭连接
    public static void close(){

        if(admin!=null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if(connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {

        //向表插入数据
        putData("Student","scofield","score","English","45");
        putData("Student","scofield","score","Math","89");
        putData("Student","scofield","score","Computer","100");

        //关闭资源
        close();
    }
}

