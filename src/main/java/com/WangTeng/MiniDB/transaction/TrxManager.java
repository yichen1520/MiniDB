package com.WangTeng.MiniDB.transaction;

import java.util.concurrent.atomic.AtomicInteger;

public class TrxManager {
    private static AtomicInteger trxIdCount = new AtomicInteger(1);

    public static Trx newTrx() {
        Trx trx = new Trx();
        trx.setTrxId(trxIdCount.getAndIncrement());
        return trx;
    }

    public static Trx newEmptyTrx() {
        Trx trx = new Trx();
        return trx;
    }
}
