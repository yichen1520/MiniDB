package com.WangTeng.MiniDB.test;


import com.WangTeng.MiniDB.engine.Database;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;


public class FilePermissionTest {

    @Test
    public void testFilePermission() {

        // 指定文件路径
        String filePath = "D:\\minidb";
        File file = new File(filePath);

        // 判断文件是否存在
        if (!file.exists()) {
            System.out.println("文件不存在！");
            return;
        }

        // 判断文件是否可读
        if (file.canRead()) {
            System.out.println("文件可读");
        } else {
            System.out.println("文件不可读");
        }

        // 判断文件是否可写
        if (file.canWrite()) {
            System.out.println("文件可写");
        } else {
            System.out.println("文件不可写");
        }

        // 判断文件是否可执行
        if (file.canExecute()) {
            System.out.println("文件可执行");
        } else {
            System.out.println("文件不可执行");
        }
    }

    /**
     * 排查Database加载时文件拒绝访问错误
     * 问题：创建文件时D:\MiniDB\log\log，第二个log应该是文件，不能是目录
     */
    @Test
    public void s() {
        try {
            RandomAccessFile file = new RandomAccessFile("D:\\MiniDB\\log\\log","rw");
//            RandomAccessFile file = new RandomAccessFile("D:\\wh.mp4","rw");
            FileChannel channel = file.getChannel();
            System.out.println(channel);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDatabaseInstance(){
        Database.getInstance();
    }
}


