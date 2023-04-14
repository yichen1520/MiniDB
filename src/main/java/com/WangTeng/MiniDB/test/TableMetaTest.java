package com.WangTeng.MiniDB.test;

import com.WangTeng.MiniDB.meta.Table;
import com.WangTeng.MiniDB.meta.TableLoader;
import com.WangTeng.MiniDB.meta.TableManager;
import com.WangTeng.MiniDB.sql.SqlExecutor;
import com.WangTeng.MiniDB.test.bptest.BasicGenTable;
import com.WangTeng.MiniDB.test.sqltest.CreateTest;
import com.WangTeng.MiniDB.test.sqltest.SelectTest;
import org.junit.Test;


public class TableMetaTest extends BasicGenTable {

    @Test
    public void metaWriteTest() {
        init();
        TableLoader tableLoader = new TableLoader();
        for (Table table : TableManager.getTableMap().values()) {
            tableLoader.writeTableMeta(table);
        }
    }

    @Test
    public void metaReadTest() {
        TableLoader tableLoader = new TableLoader();
        tableLoader.readAllTable();
        insertSome();
        SqlExecutor sqlExecutor = new SqlExecutor();
        sqlExecutor.execute(SelectTest.joinSql, null, null);
    }

    private void insertSome() {
        for (int i = 0; i < 50; i++) {
            String insertSql =
                    BasicSelectTest.insertSqlTemplate.replaceFirst("\\?", String.valueOf(i)).replaceFirst("\\?",
                            "'alchemystar" +
                            String
                                    .valueOf(i) + "'").replaceFirst("\\?", "'comment" + String.valueOf(i) + "'");
            System.out.println(insertSql);
            SqlExecutor sqlExecutor = new SqlExecutor();
            sqlExecutor.execute(insertSql, null, null);
        }

        System.out.println("insert okay");
    }

    public void init() {
        SqlExecutor executor = new SqlExecutor();
        executor.execute(CreateTest.CREATE_SQL, null, null);
    }

}
