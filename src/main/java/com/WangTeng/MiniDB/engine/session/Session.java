package com.WangTeng.MiniDB.engine.session;

import com.WangTeng.MiniDB.engine.net.handler.fronted.FrontendConnection;
import com.WangTeng.MiniDB.meta.IndexEntry;
import com.WangTeng.MiniDB.meta.Table;
import com.WangTeng.MiniDB.sql.SqlExecutor;
import com.WangTeng.MiniDB.transaction.Trx;
import com.WangTeng.MiniDB.transaction.TrxManager;

public class Session {
    // session 对应的连接
    private FrontendConnection conn;
    // 是否自动提交
    private boolean isAutoCommit;
    // 当前session下的事务
    private Trx trx;

    private SqlExecutor sqlExecutor = new SqlExecutor();

    public Session(FrontendConnection conn) {
        this.conn = conn;
        trx = TrxManager.newTrx();
    }

    public void begin() {
        trx.begin();
    }

    public void commit() {
        trx.commit();
    }

    public void rollback() {
        trx.rollback();
    }

    public void addLog(Table table, int opType, IndexEntry before, IndexEntry after) {
        trx.addLog(table, opType, before, after);
    }

    public void execute(String sql, FrontendConnection connection) {
        if (trx.trxIsNotStart()) {
            trx.begin();
        }
        sqlExecutor.execute(sql, connection, this);
    }
}
