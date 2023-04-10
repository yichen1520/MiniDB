package com.WangTeng.MiniDB.access;

import com.WangTeng.MiniDB.index.BaseIndex;
import com.WangTeng.MiniDB.index.Index;
import com.WangTeng.MiniDB.index.bp.Position;
import com.WangTeng.MiniDB.meta.Attribute;
import com.WangTeng.MiniDB.meta.IndexDesc;
import com.WangTeng.MiniDB.meta.IndexEntry;
import com.WangTeng.MiniDB.meta.value.Value;
import com.WangTeng.MiniDB.meta.value.ValueLong;

public class SecondIndexCursor extends BaseIndexCursor {
    private Index clusterIndex;

    public SecondIndexCursor(Position startPos, Position endPos,
                             boolean isEqual) {
        super(startPos, endPos, isEqual);
    }

    @Override
    public IndexEntry next() {
        IndexEntry secondIndexEntry = super.next();
        if (secondIndexEntry == null) {
            return null;
        }
        IndexEntry searchEntry = getSearchEntry(secondIndexEntry);
        // 直接找到其主键即可
        Cursor cursor = clusterIndex.searchEqual(searchEntry);
        if (cursor == null) {
            return null;
        } else {
            // 主键肯定唯一
            return cursor.next();
        }
    }

    private IndexEntry getSearchEntry(IndexEntry indexEntry) {
        // rowId肯定是二级索引的最后一个字段
        ValueLong rowId = (ValueLong) indexEntry.getValues()[indexEntry.getValues().length - 1];
        IndexEntry result = new IndexEntry(new Value[] {rowId});
        // 由于是cluster计算,所以设置为cluster desc
        Attribute primaryAttr = ((BaseIndex) clusterIndex).getIndexDesc().getPrimaryAttr();
        result.setIndexDesc(new IndexDesc(new Attribute[] {primaryAttr}));
        return result;
    }

    public Index getClusterIndex() {
        return clusterIndex;
    }

    public void setClusterIndex(Index clusterIndex) {
        this.clusterIndex = clusterIndex;
    }
}
