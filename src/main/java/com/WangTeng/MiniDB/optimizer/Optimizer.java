package com.WangTeng.MiniDB.optimizer;

import com.WangTeng.MiniDB.index.Index;
import com.WangTeng.MiniDB.meta.IndexDesc;
import com.WangTeng.MiniDB.meta.IndexEntry;
import com.WangTeng.MiniDB.meta.Table;

/**
 * 查询优化器
 */
public class Optimizer {
    private Table table;

    public Optimizer(Table table) {
        this.table = table;
    }

    public Index chooseIndex(IndexEntry entry) {
        if(entry != null && !entry.isAllNull()) {
            IndexDesc indexDesc = entry.getIndexDesc();
            // 如果包含主键id,则直接用主键id进行查询
            if (indexDesc.getPrimaryAttr() != null && entry.getValues()[indexDesc.getPrimaryAttr().getIndex()] !=
                    null) {
                return table.getClusterIndex();
            }
            // 二级索引选择器优化留待后续优化
            return table.getSecondIndexes().get(0);
        }else {
            return table.getClusterIndex();
        }
    }
}
