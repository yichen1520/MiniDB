package com.WangTeng.MiniDB.sql.parser;

import com.WangTeng.MiniDB.meta.*;
import com.WangTeng.MiniDB.meta.value.Value;
import com.WangTeng.MiniDB.meta.value.ValueLong;
import com.WangTeng.MiniDB.meta.value.ValueString;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertVisitor extends SchemaStatVisitor {

    // table表达式
    protected SQLTableSource tableSource;

    private Map<String, Integer> attributeIndexMap = new HashMap<>();

    private Map<Integer, String> indexAttributeMap = new HashMap<>();

    private List<SQLExpr> valueExpr;

    private Table table;

    public boolean visit(SQLInsertStatement x) {
        tableSource = x.getTableSource();
        if (!(tableSource instanceof SQLExprTableSource)) {
            throw new RuntimeException("not support this table source type :" + tableSource);
        }
        // 通过 tableSource 拿到插入表的表名，从 TableManager 中拿到对应的表
        table = TableManager.getTable(tableSource.toString());
        mapColumnIndex(x);
        SQLInsertStatement.ValuesClause valuesClause = x.getValues();
        // 只支持单条insert
        valueExpr = valuesClause.getValues();
        return true;
    }

    public IndexEntry buildInsertEntry() {
        Value[] values = new Value[table.getAttributes().length];
        ClusterIndexEntry indexEntry = new ClusterIndexEntry(values);
        indexEntry.setIndexDesc(new IndexDesc(table.getAttributes()));
        for (int i = 0; i < values.length; i++) {
            String attributeName = table.getAttributes()[i].getName();
            Integer index = attributeIndexMap.get(attributeName);
            if (index != null) {
                values[i] = getValue(valueExpr.get(index));
            } else {
                // 采用默认值
                values[i] = table.getAttributes()[i].getDefaultValue();
            }
        }
        return indexEntry;
    }

    public Value getValue(SQLExpr sqlExpr) {
        if (sqlExpr instanceof SQLNumericLiteralExpr) {
            return new ValueLong(((SQLNumericLiteralExpr) sqlExpr).getNumber().longValue());
        } else if (sqlExpr instanceof SQLTextLiteralExpr) {
            return new ValueString(((SQLTextLiteralExpr) sqlExpr).getText());
        }
        throw new RuntimeException("can't support not literal expr in insert");
    }

    // 组装column和index的映射
    public void mapColumnIndex(SQLInsertStatement x) {
        List<SQLExpr> list = x.getColumns();
        for (int i = 0; i < list.size(); i++) {
            SQLExpr expr = list.get(i);
            String column = getColumn(expr).getName();

            attributeIndexMap.put(column, i);
            // 记录反向映射
            indexAttributeMap.put(i, column);
        }
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }
}