package com.hunter95.dao;

import com.hunter95.constants.constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 1、分布微博
 * 2、删除微博
 * 3、关注用户
 * 4、取关用户
 * 5、获取微博用户详情
 * 6、获取用户的初始化页面
 */
public class HBaseDao {

    //1、发布微博
    public static void publishWeiBo(String uid,String content) throws IOException {

        //获取connection对象
        Connection connection = ConnectionFactory.createConnection(constants.CONFIGURATION);

        //第一部分：操作微博内容表
        //1.获取微博内容表对象
        Table contTable = connection.getTable(TableName.valueOf(constants.CONTENT_TABLE));

        //2.获取当前时间戳
        long ts=System.currentTimeMillis();

        //3.获取rowKey
        String rowKey=uid+"_"+ts;

        //4.创建put对象
        Put contPut = new Put(Bytes.toBytes(rowKey));

        //5.给put对象赋值
        contPut.addColumn(Bytes.toBytes(constants.CONTENT_TABLE_CF),Bytes.toBytes("content"),Bytes.toBytes(content));

        //6.执行插入数据操作
        contTable.put(contPut);

        //第二部分：操作微博收件箱表
        //1.获取关系表对象
        Table relaTable = connection.getTable(TableName.valueOf(constants.RELATION_TABLE));

        //2.获取当前发布微博人的fans列族数据
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(constants.RELATION_TABLE_CF2));
        Result result = relaTable.get(get);

        //3.创建一个集合，用于存放微博内容表的put对象
        ArrayList<Put> inboxPuts = new ArrayList<>();

        //4.遍历粉丝
        for (Cell cell : result.rawCells()) {

            //5.构建微博收件箱表的put对象
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));

            //6.给微博收件箱表的put对象赋值
            inboxPut.addColumn(Bytes.toBytes(constants.INBOX_TABLE_CF),Bytes.toBytes(uid),Bytes.toBytes(rowKey));

            //7.将微博收件箱表的put对象存入集合
            inboxPuts.add(inboxPut);
        }

        //8.判断是否有粉丝
        if(inboxPuts.size()>0){

            //获取收件箱表对象
            Table inboxTable = connection.getTable(TableName.valueOf(constants.INBOX_TABLE));

            //执行收件箱表数据插入操作
            inboxTable.put(inboxPuts);

            //关闭收件箱表
            inboxTable.close();
        }

        //关闭资源
        relaTable.close();
        contTable.close();
        connection.close();

    }
}
