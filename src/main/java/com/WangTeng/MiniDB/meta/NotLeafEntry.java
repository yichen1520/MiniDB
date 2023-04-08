package com.WangTeng.MiniDB.meta;

import com.WangTeng.MiniDB.meta.value.Value;

public class NotLeafEntry extends IndexEntry {

    public NotLeafEntry(Value[] values) {
        super(values);
    }

    // cluster的非叶子节点,其本身就是compare key
    public IndexEntry getCompareEntry() {
        return this;
    }

}
