package com.WangTeng.MiniDB.test.sqltest;

import com.WangTeng.MiniDB.sql.SqlExecutor;
import com.WangTeng.MiniDB.test.BasicSelectTest;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import org.junit.Test;


public class DeleteTest extends BasicSelectTest {

    public static final String deleteSql = "delete from test where id>=1";

    @Test
    public void test() {
        SqlExecutor executor = new SqlExecutor();
        executor.execute(deleteSql, null, null);
    }

    @Test
    public void test2(){
        System.out.println("+==============================+");
        String s = "insert into INTO `android_message_all` (`msg_payload`,`appkey`,`live_time`,`createtime`) VALUES (\"\\\\'\",\"\\'\",\"\\'\",\"\\'\");";
        System.out.println(s);
        SQLStatementParser insertStatement = new SQLStatementParser(s);
        System.out.println(insertStatement);
    }
}
