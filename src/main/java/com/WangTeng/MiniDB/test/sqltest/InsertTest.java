package com.WangTeng.MiniDB.test.sqltest;

import com.WangTeng.MiniDB.meta.Table;
import com.WangTeng.MiniDB.meta.TableManager;
import com.WangTeng.MiniDB.sql.SqlExecutor;
import com.WangTeng.MiniDB.test.bptest.BasicGenTable;
import org.junit.Before;
import org.junit.Test;


public class InsertTest extends BasicGenTable {

    public static final String insertSqlTemplate = "insert into test (id,name,comment) values (?,?,?)";

    @Before
    public void init() {
        Table table = genTable();
        TableManager.addTable(table, false);
    }

    @Test
    public void test() {
        for (int i = 0; i < 1000; i++) {
            String insertSql = insertSqlTemplate.replace("?", String.valueOf(i)).replace("?", "alchemystar" + String
                    .valueOf(i)
                    + "comment" + String.valueOf(i));
            SqlExecutor sqlExecutor = new SqlExecutor();
            sqlExecutor.execute(insertSql, null, null);
        }

        System.out.println("insert okay");
    }
}
