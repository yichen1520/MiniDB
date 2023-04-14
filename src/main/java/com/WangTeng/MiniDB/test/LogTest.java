package com.WangTeng.MiniDB.test;

import com.WangTeng.MiniDB.engine.session.Session;
import com.WangTeng.MiniDB.engine.session.SessionFactory;
import com.WangTeng.MiniDB.sql.SqlExecutor;
import com.WangTeng.MiniDB.store.log.LogStore;
import com.WangTeng.MiniDB.test.sqltest.CreateTest;
import com.WangTeng.MiniDB.transaction.log.Log;
import org.junit.Test;

import java.util.List;


public class LogTest {

    public static final String insertSqlTemplate = "insert into test (id,name) values (?,?)";

    @Test
    public void testWrite() {
        SqlExecutor executor = new SqlExecutor();
        executor.execute(CreateTest.CREATE_SQL, null, null);
        insertSome();
    }

    @Test
    public void testRead() {
        LogStore logStore = new LogStore();
        List<Log> list = logStore.loadLog();
        System.out.println(list);
    }

    private void insertSome() {
        Session session = SessionFactory.newSession(null);
        for (int i = 0; i < 50; i++) {
            String insertSql =
                    insertSqlTemplate.replaceFirst("\\?", String.valueOf(i)).replaceFirst("\\?", "'alchemystar" +
                            String
                                    .valueOf(i) + "'").replaceFirst("\\?", "'comment" + String.valueOf(i) + "'");
            System.out.println(insertSql);
            SqlExecutor sqlExecutor = new SqlExecutor();
            sqlExecutor.execute(insertSql, null, session);
        }
        session.commit();

        System.out.println("insert okay");
    }

}
