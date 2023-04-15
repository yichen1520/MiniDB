package com.WangTeng.MiniDB.meta;

import com.WangTeng.MiniDB.engine.Database;
import com.WangTeng.MiniDB.sql.parser.SelectVisitor;
import com.WangTeng.MiniDB.sql.select.TableFilter;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;

import java.util.HashMap;
import java.util.Map;

public class TableManager {
    public static Map<String, Table> tableMap = new HashMap<>();

    public static TableFilter newTableFilter(SQLExprTableSource sqlExprTableSource, SelectVisitor selectVisitor) {
        TableFilter tableFilter = new TableFilter();
        String tableName = sqlExprTableSource.getExpr().toString();
        Table table = tableMap.get(tableName);
        tableFilter.setTable(table);
        tableFilter.setSelectVisitor(selectVisitor);
        tableFilter.setAlias(sqlExprTableSource.getAlias());
        tableFilter.setFilterCondition(selectVisitor.getWhereCondition());
        return tableFilter;
    }

    public static TableFilter newTableFilter(SQLExprTableSource sqlExprTableSource, SQLExpr whereExpr) {
        TableFilter tableFilter = new TableFilter();
        String tableName = sqlExprTableSource.getExpr().toString();
        Table table = tableMap.get(tableName);
        tableFilter.setTable(table);
        tableFilter.setAlias(sqlExprTableSource.getAlias());
        tableFilter.setFilterCondition(whereExpr);
        return tableFilter;
    }

    public static Table getTable(String tableName) {
        Table table = tableMap.get(tableName);
        if (table == null) {
            throw new RuntimeException("not found this table , tableName = " + tableName);
        }
        return table;
    }

    public static Table getTableWithNoException(String tableName) {
        return tableMap.get(tableName);
    }

    public static void addTable(Table table, boolean isPersist) {
        if (tableMap.get(table.getName()) != null) {
            throw new RuntimeException("table " + table.getName() + " already exists");
        }
        if (isPersist) {
            // 先落盘,再写入
            Database database = Database.getInstance();
            TableLoader loader = database.getTableLoader();
            loader.writeTableMeta(table);
        }
        tableMap.put(table.getName(), table);
    }

    public static Map<String, Table> getTableMap() {
        return tableMap;
    }

    public static void setTableMap(Map<String, Table> tableMap) {
        TableManager.tableMap = tableMap;
    }
}
