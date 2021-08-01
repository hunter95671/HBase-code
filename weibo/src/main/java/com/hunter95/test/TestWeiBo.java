package com.hunter95.test;

import com.hunter95.constants.constants;
import com.hunter95.dao.HBaseDao;
import com.hunter95.utils.HBaseUtil;

import java.io.IOException;

public class TestWeiBo {

    public static void init(){

        try {
            //创建命名空间
            HBaseUtil.createNameSpace(constants.NAMESPACE);
            //创建微博内容表
            HBaseUtil.createTable(constants.CONTENT_TABLE,constants.CONTENT_TABLE_VERSIONS,constants.CONTENT_TABLE_CF);
            //创建用户关系表
            HBaseUtil.createTable(constants.RELATION_TABLE,constants.RELATION_TABLE_VERSIONS,constants.RELATION_TABLE_CF1,constants.RELATION_TABLE_CF2);
            //创建收件箱表
            HBaseUtil.createTable(constants.INBOX_TABLE,constants.INBOX_TABLE_VERSIONS,constants.INBOX_TABLE_CF);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        //初始化
        init();
        //1001发布微博
        HBaseDao.publishWeiBo("1001","好耶！");

        //1002关注1001和1003
        HBaseDao.addAttends("1002","1001","1003");

        //获取1002初始化页面
        HBaseDao.getInit("1002");

        System.out.println("+++++++++++++++++++++++++++");

        //1003发布三条微博，同时1001发布两条微博
        HBaseDao.publishWeiBo("1003","ok！");
        Thread.sleep(10);
        HBaseDao.publishWeiBo("1003","hi！");
        Thread.sleep(10);
        HBaseDao.publishWeiBo("1003","hello！");
        Thread.sleep(10);
        HBaseDao.publishWeiBo("1001","好的！");
        Thread.sleep(10);
        HBaseDao.publishWeiBo("1001","好呀！");

        //获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("+++++++++++++++++++++++++++");

        //1002取关1003
        HBaseDao.deleteAttends("1002","1003");

        //获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("+++++++++++++++++++++++++++");

        //1002再次关注1003
        HBaseDao.addAttends("1002","1003");

        //获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("+++++++++++++++++++++++++++");

        //获取1001微博详情
        HBaseDao.getWeiBo("1001");
    }
}
