package com.WangTeng.MiniDB.test.bptest;

import com.WangTeng.MiniDB.config.SystemConfig;
import com.WangTeng.MiniDB.meta.IndexEntry;
import com.WangTeng.MiniDB.meta.value.*;
import com.WangTeng.MiniDB.store.fs.FStore;
import com.WangTeng.MiniDB.store.item.Item;
import com.WangTeng.MiniDB.store.page.Page;
import com.WangTeng.MiniDB.store.page.PageLoader;
import com.WangTeng.MiniDB.store.page.PagePool;
import org.junit.Test;

public class PageTest {

    @Test
    public void pageTest() {
        Value[] values = new Value[5];
        values[0] = new ValueString("this is freedom db");
        values[1] = new ValueString("just enjoy it");
        values[2] = new ValueBoolean(true);
        values[3] = new ValueInt(5);
        values[4] = new ValueLong(6L);
        IndexEntry indexEntry = new IndexEntry(values);
        Item item = new Item(indexEntry);
        System.out.println(item.getLength());
        PagePool pagePool = PagePool.getIntance();
        Page page = pagePool.getFreePage();
        for (int i = 0; i < 1000; i++) {
            if (page.writeItem(item)) {
                continue;
            } else {
                System.out.println("btee=" + i + ",page size exhaust");
                break;
            }
        }
        FStore fStore = new FStore(SystemConfig.MINIDB_REL_DATA_PATH);
        fStore.open();
        fStore.writePageToFile(page, 0);
        fStore.writePageToFile(page, 10);

        PageLoader loader = fStore.readPageLoaderFromFile(0);
        IndexEntry[] indexEntries = loader.getIndexEntries();
        for (int i = 0; i < indexEntries.length; i++) {
            System.out.println(indexEntries[i]);
        }
    }
}
