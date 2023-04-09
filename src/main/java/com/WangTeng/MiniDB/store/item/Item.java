package com.WangTeng.MiniDB.store.item;

import com.WangTeng.MiniDB.meta.IndexEntry;
import com.WangTeng.MiniDB.store.page.Page;

/**
 * item指查询结果集（query result set）中的一个单元
 * 它是一个交叉点，表示某一行和某一列相交处的值
 */
public class Item {
    private ItemPointer ptr;
    private ItemData data;

    public Item(IndexEntry indexEntry) {
        data = new ItemData(indexEntry);
        ptr = new ItemPointer(0, data.getLength());
    }

    // 写入item,如果空间不够,返回false
    public boolean writeItem(Page page) {
        int freeSpace = page.remainFreeSpace();
        if (freeSpace < getLength()) {
            return false;
        }
        // 顺序必须如此,只有写入data之后
        // 才能知道ptr中的offset
        data.write(page);
        // 修正ptr的offset
        ptr.setOffset(data.getOffset());
        ptr.write(page);
        page.addTupleCount(page);
        return true;
    }

    public int getLength() {
        return data.getLength() + ptr.getPtrLength();
    }

    public static int getItemLength(IndexEntry key) {
        return key.getLength() + 8;
    }
}
