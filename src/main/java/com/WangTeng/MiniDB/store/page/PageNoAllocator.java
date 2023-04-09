package com.WangTeng.MiniDB.store.page;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 页码分配器
 */
public class PageNoAllocator {
    // 已经被使用的页数
    private AtomicInteger count;

    // 已经被回收的空闲页号列表
    private List<Integer> freePageNoList;

    public PageNoAllocator() {
        // 0 for meta page
        count = new AtomicInteger(1);
        freePageNoList = new LinkedList<>();
    }

    public int getNextPageNo() {
        if (freePageNoList.size() == 0) {
            return count.getAndAdd(1);
        }
        return freePageNoList.remove(0);
    }

    public void recycleCount(int pageNo) {
        freePageNoList.add(pageNo);
    }

    // 从磁盘中,重新构造page的时候,需要重新设置其pageNo
    public void setCount(int lastPageNo) {
        count.set(lastPageNo + 1);
    }
}
