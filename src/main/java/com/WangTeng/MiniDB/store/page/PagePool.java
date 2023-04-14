package com.WangTeng.MiniDB.store.page;

import java.util.AbstractQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PagePool {
    private static PagePool pagePool;

    // 默认页数
    private static int defaultPageNum = 8;
    // 可用page
    private AbstractQueue<Page> frees = new ConcurrentLinkedQueue<>();
    // page工厂
    private PageFactory factory = PageFactory.getInstance();

    static {
        pagePool = new PagePool();
        pagePool.init();
    }

    public void init() {
        // 初始化8页的数据
        for (int i = 0; i < defaultPageNum; i++) {
            frees.add(factory.newPage());
        }
    }

    public static PagePool getIntance() {
        return pagePool;
    }

    public Page getFreePage() {
        //  Page page = frees.poll();
        //  if (page == null) {
        return factory.newPage();
        //  }
        //  return page;
    }

    public void recycle(Page page) {
        page.clean();
        frees.add(page);
    }
}
