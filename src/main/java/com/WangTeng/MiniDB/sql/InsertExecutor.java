package com.WangTeng.MiniDB.sql;

import com.WangTeng.MiniDB.engine.session.Session;
import com.WangTeng.MiniDB.meta.IndexEntry;
import com.WangTeng.MiniDB.sql.parser.InsertVisitor;
import com.WangTeng.MiniDB.transaction.OpType;
import com.alibaba.druid.sql.ast.SQLStatement;

public class InsertExecutor {
    private SQLStatement sqlStatement;

    private InsertVisitor insertVisitor;

    public InsertExecutor(SQLStatement sqlStatement) {
        this.sqlStatement = sqlStatement;
    }

    public void execute(Session session) {
        init();
        // 必须支持带主键
        IndexEntry indexEntry = insertVisitor.buildInsertEntry();
        if (session != null) {
            session.addLog(insertVisitor.getTable(), OpType.insert, null, indexEntry);
        }
        // 先落盘再对索引做操作
        insertVisitor.getTable().insert(indexEntry);
    }

    public void init() {
        InsertVisitor insertVisitor = new InsertVisitor();
        sqlStatement.accept(insertVisitor);
        this.insertVisitor = insertVisitor;
    }
}
