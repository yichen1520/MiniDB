package com.WangTeng.MiniDB.sql;

import com.WangTeng.MiniDB.meta.TableManager;
import com.WangTeng.MiniDB.sql.parser.CreateVisitor;
import com.alibaba.druid.sql.ast.SQLStatement;

public class CreateExecutor {
    private SQLStatement sqlStatement;

    private CreateVisitor createVisitor;

    public CreateExecutor(SQLStatement sqlStatement) {
        this.sqlStatement = sqlStatement;
    }

    public void execute() {
        init();
        TableManager.addTable(createVisitor.getTable(), true);
    }

    public void init() {
        CreateVisitor createVisitor = new CreateVisitor();
        // 将 SQL 语句传递给 createVisitor 方法进行访问，具体执行计划将会由 createVisitor 方法来决定
        sqlStatement.accept(createVisitor);
        this.createVisitor = createVisitor;
    }
}
