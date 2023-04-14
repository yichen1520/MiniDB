package com.WangTeng.MiniDB.sql;

import com.WangTeng.MiniDB.engine.net.handler.fronted.FrontendConnection;
import com.WangTeng.MiniDB.engine.net.response.OkResponse;
import com.WangTeng.MiniDB.engine.session.Session;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;

/**
 * sql 解析的主类，将sql传入使用druid生成SQLStatement交给对应的执行器执行
 */
public class SqlExecutor {
    public static final long ONE = 1;

    public void execute(String sql, FrontendConnection con, Session session) {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement sqlStatement = parser.parseStatement();
        if (sqlStatement instanceof SQLCreateTableStatement) {
            CreateExecutor createExecutor = new CreateExecutor(sqlStatement);
            createExecutor.execute();
            if (con != null) {
                OkResponse.response(con);
            }
            return;
        } else if (sqlStatement instanceof SQLInsertStatement) {
            InsertExecutor insertExecutor = new InsertExecutor(sqlStatement);
            insertExecutor.execute(session);
            if (con != null) {
                OkResponse.responseWithAffectedRows(con, ONE);
            }
            return;
        } else if (sqlStatement instanceof SQLSelectStatement) {
            SelectExecutor selectExecutor = new SelectExecutor(sqlStatement, con);
            selectExecutor.execute();
            return;
        } else if (sqlStatement instanceof SQLDeleteStatement) {
            DeleteExecutor deleteExecutor = new DeleteExecutor(sqlStatement, con);
            deleteExecutor.execute(session);
            return;
        } else {
            throw new RuntimeException("not support this statement " + sqlStatement);
        }
    }
}
