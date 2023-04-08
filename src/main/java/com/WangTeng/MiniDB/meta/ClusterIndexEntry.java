package com.WangTeng.MiniDB.meta;

import com.WangTeng.MiniDB.meta.value.Value;
import com.WangTeng.MiniDB.meta.value.ValueLong;

/**
 * 聚簇索引项
 */
public class ClusterIndexEntry extends IndexEntry {

    public ClusterIndexEntry() {
    }

    public ClusterIndexEntry(Value[] values) {
        super(values);
    }

    // cluster 的 compareEntry key 就是主键
    public IndexEntry getCompareEntry() {
        if (compareEntry == null) {
            compareEntry = new NotLeafEntry(new Value[]{getRowId()});
            compareEntry.setIndexDesc(new IndexDesc(new Attribute[]{indexDesc.getPrimaryAttr()}));
        }
        return compareEntry;
    }

    public IndexEntry getDeleteCompareEntry() {
        return getCompareEntry();
    }

    public ValueLong getRowId() {
        return (ValueLong) values[indexDesc.getPrimaryAttr().getIndex()];
    }
}
