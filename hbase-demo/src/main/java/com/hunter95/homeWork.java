package com.hunter95;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class homeWork {

    private static Connection connection = null;
    private static Admin admin = null;

    static {
        try {
            //1.获取配置信息
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

            //2.创建连接对象
            connection = ConnectionFactory.createConnection(configuration);

            //3.创建admin对象
            admin = connection.getAdmin();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {

        //判断表是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        //返回结果
        return exists;
    }

    //创建表
    public static void createTable(String tableName, String... cfs) throws IOException {

        //1.判断是否存在列族信息
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息！");
            return;
        }

        //2.判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
            return;
        }

        //3.创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        //4.循环添加列族信息
        for (String cf : cfs) {

            //5.创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            //6.添加具体列族信息
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        //7.创建表
        admin.createTable(hTableDescriptor);
    }

    //向表插入数据
    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {

        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //3.给put对象赋值
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));

        //4.插入数据
        table.put(put);

        //5.关闭连接
        table.close();

    }

    //关闭连接
    public static void close() {

        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {

        createTable("201906061515_emp", "201906061515_info1", "201906061515_info2");
        //向表插入数据;
        String[] nameArray = {"Roswell",
                "Quincy",
                "Vernon",
                "Ian",
                "Edmund",
                "Lucas",
                "Godly",
                "Joe",
                "Ernest",
                "Frank",
                "Gloria",
                "Mona",
                "Emily",
                "Anne",
                "Idelle",
                "Verda",
                "Renata",
                "Ann",
                "Handmaiden",
                "Shining",
                "Shawn",
                "Randolph",
                "Elton",
                "Ramsey",
                "Valley",
                "Quinlan",
                "Erskine",
                "Joey",
                "Keene",
                "Unwin",
                "Compassionate",
                "Joan",
                "Song-Thrush",
                "Eudora",
                "Roberta",
                "Marilyn",
                "Gertrude",
                "Trixie",
                "Prunella",
                "Holly",
                "Tony",
                "Ethanael",
                "Silas",
                "Lee",
                "Fairfax",
                "Free",
                "Garret",
                "Stan",
                "Darell",
                "Wilbur"};

        for (int i = 0; i < nameArray.length; i++) {
            putData("201906061515_emp",""+i, "201906061515_info1", "ename", nameArray[i]);
            putData("201906061515_emp",""+i, "201906061515_info1", "salary", ""+(8000+i));
            putData("201906061515_emp",""+i, "201906061515_info2", "deptno", "0"+(((i)%5)+1));
        }
        //关闭资源
        close();
    }
}

