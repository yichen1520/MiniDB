package com.WangTeng.MiniDB.meta;

import com.WangTeng.MiniDB.config.SystemConfig;
import com.WangTeng.MiniDB.index.BaseIndex;
import com.WangTeng.MiniDB.index.bp.BPTree;
import com.WangTeng.MiniDB.meta.value.Value;
import com.WangTeng.MiniDB.meta.value.ValueInt;
import com.WangTeng.MiniDB.meta.value.ValueString;
import com.WangTeng.MiniDB.store.fs.FStore;
import com.WangTeng.MiniDB.store.item.Item;
import com.WangTeng.MiniDB.store.page.Page;
import com.WangTeng.MiniDB.store.page.PageLoader;
import com.WangTeng.MiniDB.store.page.PagePool;
import com.WangTeng.MiniDB.util.ValueConvertUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TableLoader {

    public static final int META_PAGE_INDEX = 0;

    public void readAllTable() {
        String path = SystemConfig.MINIDB_REL_META_PATH;
        File file = new File(path);
        for (File fileItem : file.listFiles()) {
            readTableMeta(fileItem.getPath());
        }
    }

    /**
     * 读取表的元数据信息
     * size Attribute[] 索引数量 索引信息
     * @param path
     */
    public void readTableMeta(String path) {
        FStore metaStore = new FStore(path);
        Table table = new Table();
        // 元信息只有一页
        PageLoader loader = metaStore.readPageLoaderFromFile(0);
        List<Attribute> list = new ArrayList<>();
        // 第一个项是除index之外的meta大小
        int size = loader.getIndexEntries()[0].getValues()[0].getInt();
        String name = loader.getIndexEntries()[1].getValues()[0].getString();
        table.setName(name);
        for (int i = 2; i <= size; i++) {
            IndexEntry entry = loader.getIndexEntries()[i];
            Attribute attr = ValueConvertUtil.convertValue(entry.getValues());
            list.add(attr);
        }
        Attribute[] attributes = list.toArray(new Attribute[list.size()]);
        table.setAttributes(attributes);

        int indexCount = loader.getIndexEntries()[size + 1].getValues()[0].getInt();
        // 第一个是clusterIndex
        int position = size + 2;
        for (int i = 0; i < indexCount; i++) {
            //从页中逐个读取indexCount数量的索引信息
            position = readOneIndex(table, loader.getIndexEntries(), position);
        }

        TableManager.addTable(table, false);
    }

    // 读并设置startPosition和index
    public int readOneIndex(Table table, IndexEntry[] indexEntries, int startPosition) {
        int primaryItemSize = indexEntries[startPosition].getValues()[0].getInt();
        List<Attribute> list = new ArrayList<>();
        String name = indexEntries[startPosition + 1].getValues()[0].getString();
        boolean isUnique = indexEntries[startPosition + 2].getValues()[0].getInt() > 0 ? true : false;
        boolean isPrimaryKey = indexEntries[startPosition + 3].getValues()[0].getInt() > 0 ? true : false;
        for (int i = (startPosition + 4); i < startPosition + primaryItemSize + 1; i++) {
            IndexEntry entry = indexEntries[i];
            Attribute attr = ValueConvertUtil.convertValue(entry.getValues());
            list.add(attr);
        }

        BPTree bpTree = new BPTree(table, name, list.toArray(new Attribute[] {}));
        bpTree.setUnique(isUnique);
        bpTree.setPrimaryKey(isPrimaryKey);
        if (table.getClusterIndex() == null) {
            table.setClusterIndex(bpTree);
        } else {
            table.getSecondIndexes().add(bpTree);
        }
        return startPosition + primaryItemSize + 1;
    }

    // 对进行表持久化
    public void writeTableMeta(Table table) {
        String name = table.getName();
        // tableName
        Item itemName = new Item(new IndexEntry(new Value[] {new ValueString(name)}));
        // for attribute
        List<Item> metaItems = table.getItems();
        Item itemSize = new Item(new IndexEntry(new Value[] {new ValueInt(1 + metaItems.size())}));
        Page page = PagePool.getIntance().getFreePage();
        page.writeItem(itemSize);
        page.writeItem(itemName);
        page.writeItems(metaItems);
        int indexCount = 1 + table.getSecondIndexes().size();
        Item indexCountItem = new Item(new IndexEntry(new Value[] {new ValueInt(indexCount)}));
        page.writeItem(indexCountItem);
        // for indexes
        page.writeItems(table.getClusterIndex().getItems());
        for (BaseIndex baseIndex : table.getSecondIndexes()) {
            page.writeItems(baseIndex.getItems());
        }
        table.getMetaStore().writePageToFile(page, META_PAGE_INDEX);
    }
}
