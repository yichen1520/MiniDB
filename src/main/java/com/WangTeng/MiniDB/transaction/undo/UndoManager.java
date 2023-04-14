package com.WangTeng.MiniDB.transaction.undo;

import com.WangTeng.MiniDB.meta.Table;
import com.WangTeng.MiniDB.meta.TableManager;
import com.WangTeng.MiniDB.transaction.OpType;
import com.WangTeng.MiniDB.transaction.log.Log;
import com.WangTeng.MiniDB.transaction.log.LogType;

public class UndoManager {
    public static void undo(Log log) {
        Table table = TableManager.getTable(log.getTableName());
        if (log.getLogType() == LogType.ROW) {
            switch (log.getOpType()) {
                case OpType.insert:
                    undoInsert(table, log);
                    break;
                case OpType.update:
                    undoUpdate(table, log);
                    break;
                case OpType.delete:
                    undoDelete(table, log);
                    break;
            }
        } else {
            // do nothing;
        }
    }

    public static void undoInsert(Table table, Log log) {
        // insert undo = > delete
        table.delete(log.getAfter());
    }

    public static void undoUpdate(Table table, Log log) {
        // todo
    }

    public static void undoDelete(Table table, Log log) {
        table.insert(log.getBefore());
    }
}
