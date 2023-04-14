package com.WangTeng.MiniDB.transaction.log;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 序列号生成工厂
 */
public class LSNFactory {
    private static AtomicLong lsnAllocator = new AtomicLong(0);

    public static long nextLSN() {
        return lsnAllocator.getAndIncrement();
    }
}
