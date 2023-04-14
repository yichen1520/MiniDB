package com.WangTeng.MiniDB.meta;

import com.WangTeng.MiniDB.access.Cursor;
import com.WangTeng.MiniDB.config.SystemConfig;
import com.WangTeng.MiniDB.index.BaseIndex;
import com.WangTeng.MiniDB.index.Index;
import com.WangTeng.MiniDB.meta.value.Value;
import com.WangTeng.MiniDB.optimizer.Optimizer;
import com.WangTeng.MiniDB.store.fs.FStore;
import com.WangTeng.MiniDB.store.item.Item;
import com.WangTeng.MiniDB.util.ValueConvertUtil;

import java.util.*;

public class Table {
    // table名称
    private String name;
    // relation包含的元组描述
    // 二级索引的最后一个属性是 rowId：主键索引对应的相关信息
    private Attribute[] attributes;
    // 属性map   key: attributes.name  value: Attribute[i]中的下标i
    // 用于使用druid中的Visitor类处理sql语句生成的语法树新建表初始化二级索引，快速查找对应下标
    private Map<String, Integer> attributesMap;
    // 主键属性
    private Attribute primaryAttribute;
    // 数据表所对应的文件路径
    private String tablePath;
    // 数据表元信息所对应的文件路径
    private String metaPath;
    // 装载数据表具体数据信息（即记录）的存储对象
    private FStore tableStore;
    // 装载数据表元信息的存储对象
    private FStore metaStore;
    // 主键索引,聚簇索引
    private BaseIndex clusterIndex;
    // second索引 二级索引
    private List<BaseIndex> secondIndexes = new ArrayList<>();

    private Optimizer optimizer;

    public Table() {
        optimizer = new Optimizer(this);
    }

    public Cursor searchEqual(IndexEntry entry) {
        Index chooseIndex = optimizer.chooseIndex(entry);
        return chooseIndex.searchEqual(entry);
    }

    public Cursor searchRange(IndexEntry lowKey, IndexEntry upKey) {
        Index chooseIndex = optimizer.chooseIndex(lowKey);
        return chooseIndex.searchRange(lowKey, upKey);
    }

    // CRUD
    public void insert(IndexEntry entry) {
        // 插入聚簇索引
        clusterIndex.insert(entry, true);
        // 二级索引的插入
        for (BaseIndex secondIndex : secondIndexes) {
            secondIndex.insert(entry, false);
        }
    }

    public void delete(IndexEntry entry) {
        // 删除聚簇索引
        clusterIndex.delete(entry);
        for (BaseIndex secondIndex : secondIndexes) {
            secondIndex.delete(entry);
        }
    }

    public int getAttributeIndex(String name) {
        return attributesMap.get(name);
    }

    public Attribute[] getAttributes() {
        return attributes;
    }

    public void setAttributes(Attribute[] attributes) {
        this.attributes = attributes;
        attributesMap = new HashMap<>();
        for (int i = 0; i < attributes.length; i++) {
            attributesMap.put(attributes[i].getName(), i);
            if (attributes[i].isPrimaryKey()) {
                primaryAttribute = attributes[i];
            }
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
        if (metaPath == null) {
            metaPath = SystemConfig.MINIDB_REL_META_PATH + "/" + name;
        }
        if (tablePath == null) {
            tablePath = SystemConfig.MINIDB_REL_DATA_PATH + "/" + name;
        }
    }

    public BaseIndex getClusterIndex() {
        return clusterIndex;
    }

    public void setClusterIndex(BaseIndex clusterIndex) {
        this.clusterIndex = clusterIndex;
    }

    public List<BaseIndex> getSecondIndexes() {
        return secondIndexes;
    }

    public void setSecondIndexes(List<BaseIndex> secondIndexes) {
        this.secondIndexes = secondIndexes;
    }

    public Attribute getPrimaryAttribute() {
        return primaryAttribute;
    }

    public void setPrimaryAttribute(Attribute primaryAttribute) {
        this.primaryAttribute = primaryAttribute;
    }

    // todo 先不考虑持久化
    public void loadFromDisk() {
        // 先不考虑持久化
    }

    /**
     * 刷回磁盘
     */
    public void flushDataToDisk() {
        clusterIndex.flushToDisk();
        for (BaseIndex baseIndex : secondIndexes) {
            baseIndex.flushToDisk();
        }
    }

    public List<Item> getItems() {
        List<Item> list = new LinkedList<>();
        for (Attribute attribute : attributes) {
            //索引元组使用数组存储，需要利用工具类把Attribute转换成数组，new IndexEntry
            Value[] values = ValueConvertUtil.convertAttr(attribute);
            IndexEntry tuple = new IndexEntry(values);
            Item item = new Item(tuple);
            list.add(item);
        }
        return list;
    }

    public FStore getMetaStore() {
        if (metaStore == null) {
            metaStore = new FStore(metaPath);
        }
        return metaStore;
    }

    public void setMetaStore(FStore metaStore) {
        this.metaStore = metaStore;
    }
}
