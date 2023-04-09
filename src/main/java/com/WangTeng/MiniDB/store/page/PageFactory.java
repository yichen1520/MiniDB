package com.WangTeng.MiniDB.store.page;

import com.WangTeng.MiniDB.config.SystemConfig;
import com.WangTeng.MiniDB.index.bp.BPNode;
import com.WangTeng.MiniDB.index.bp.BpPage;

public class PageFactory {
    private static PageFactory factory = new PageFactory();

    public static PageFactory getInstance() {
        return factory;
    }

    private PageFactory() {
    }

    // 4KB
    public Page newPage() {
        return new Page(SystemConfig.DEFAULT_PAGE_SIZE);
    }

    public BpPage newBpPage(BPNode bpNode) {
        return new BpPage(bpNode);
    }
}
