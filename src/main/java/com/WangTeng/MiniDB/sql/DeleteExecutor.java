package com.WangTeng.MiniDB.sql;

import com.WangTeng.MiniDB.engine.net.handler.fronted.FrontendConnection;
import com.WangTeng.MiniDB.engine.net.response.OkResponse;
import com.WangTeng.MiniDB.engine.session.Session;
import com.WangTeng.MiniDB.meta.IndexEntry;
import com.WangTeng.MiniDB.meta.Table;
import com.WangTeng.MiniDB.meta.value.Value;
import com.WangTeng.MiniDB.meta.value.ValueBoolean;
import com.WangTeng.MiniDB.sql.parser.DeleteVisitor;
import com.WangTeng.MiniDB.sql.select.TableFilter;
import com.WangTeng.MiniDB.sql.select.item.SelectExprEval;
import com.WangTeng.MiniDB.transaction.OpType;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.util.ArrayList;
import java.util.List;

public class DeleteExecutor {
    private SQLStatement sqlStatement;

    private DeleteVisitor deleteVisitor;

    private Table table;

    private FrontendConnection con;

    public DeleteExecutor(SQLStatement sqlStatement, FrontendConnection con) {
        this.sqlStatement = sqlStatement;
        this.con = con;
    }

    public void execute(Session session) {
        init();
        TableFilter tableFilter = deleteVisitor.getTableFilter();
        List<IndexEntry> toDelete = new ArrayList<>();
        // 必须先拿出来再删除,不然会引起删除的position变化
        while (tableFilter.next()) {
            if (checkWhereCondition()) {
                toDelete.add(tableFilter.getCurrent());
            }
        }
        for (IndexEntry delItem : toDelete) {
            if (session != null) {
                session.addLog(table, OpType.delete, delItem, null);
            }
            // 先落盘,再对table做操作
            table.delete(delItem);
        }
        OkResponse.responseWithAffectedRows(con, toDelete.size());
    }

    private boolean checkWhereCondition() {
        SelectExprEval eval = new SelectExprEval(deleteVisitor.getWhere(), null);
        eval.setSimpleTableFilter(deleteVisitor.getTableFilter());
        Value value = eval.eval();
        if (value instanceof ValueBoolean) {
            return ((ValueBoolean) value).getBoolean();
        }
        throw new RuntimeException("where condition eval not boolean , wrong");
    }

    public void init() {
        DeleteVisitor deleteVisitor = new DeleteVisitor();
        sqlStatement.accept(deleteVisitor);
        this.deleteVisitor = deleteVisitor;
        this.table = deleteVisitor.getTableFilter().getTable();
    }
}
