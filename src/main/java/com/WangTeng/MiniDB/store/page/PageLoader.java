package com.WangTeng.MiniDB.store.page;

import com.WangTeng.MiniDB.meta.IndexEntry;
import com.WangTeng.MiniDB.store.item.ItemPointer;

import java.util.ArrayList;
import java.util.List;

/**
 * 存储了一页page中的bufferWrapper，并记录元组的数量
 */
public class PageLoader {

    Page page;
    /**
     * 结合BpPage中的 writeToPage 方法看
     * indexEntries中前五个IndexEntry中的Value[0] 通常表示一些属性信息，
     *      如：isLeaf、isRoot、pageNo、parentPageNo、entryCount
     * 下标从 5 开始存入节点关键字，entryCount表示要存入的数量
     * 若不是叶子节点，indexEntries[5 + entryCount]的首元素存的是 childCount
     * 若是叶子节点，indexEntries[5 + entryCount]的首元素存的是 前置节点页号，索引下一位置是后置节点页号
     */
    private IndexEntry[] indexEntries;
    private int tupleCount;

    public PageLoader(Page page) {
        this.page = page;
    }

    public void load() {
        PageHeaderData pageHeaderData = PageHeaderData.read(page);
        tupleCount = pageHeaderData.getTupleCount();
        int ptrStartOff = pageHeaderData.getLength();
        // 首先建立存储tuple的数组
        List<IndexEntry> temp = new ArrayList<>();
        // 循环读取
        for (int i = 0; i < tupleCount; i++) {
            // 重新从page读取tuple
            ItemPointer ptr = new ItemPointer(page.readInt(), page.readInt());
            if (ptr.getTupleLength() == -1) {
                continue;
            }
            byte[] bb = page.readBytes(ptr.getOffset(), ptr.getTupleLength());
            IndexEntry indexEntry = new IndexEntry();
            indexEntry.read(bb);
            temp.add(indexEntry);
            // 进入到下一个元组位置
            ptrStartOff = ptrStartOff + ptr.getTupleLength();
        }
        // 由于可能由于被删除,置为-1,所以以temp为准
        indexEntries = temp.toArray(new IndexEntry[temp.size()]);
        tupleCount = temp.size();
    }

    public IndexEntry[] getIndexEntries() {
        return indexEntries;
    }

    public int getTuplCount() {
        return tupleCount;
    }
}
