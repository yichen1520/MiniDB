package com.WangTeng.MiniDB.transaction.redo;

import com.WangTeng.MiniDB.meta.*;
import com.WangTeng.MiniDB.transaction.OpType;
import com.WangTeng.MiniDB.transaction.log.Log;

public class RedoManager {
    public static void redo(Log log) {
        Table table = TableManager.getTable(log.getTableName());
        switch (log.getOpType()) {
            case OpType.insert:
                IndexEntry indexEntry = new ClusterIndexEntry(log.getAfter().getValues());
                indexEntry.setIndexDesc(new IndexDesc(table.getAttributes()));
                table.insert(indexEntry);
                break;
            case OpType.delete:
                table.delete(log.getBefore());
                break;
            case OpType.update:
                // todo
                break;
        }
    }
}
