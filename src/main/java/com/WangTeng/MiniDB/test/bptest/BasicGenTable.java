package com.WangTeng.MiniDB.test.bptest;

import com.WangTeng.MiniDB.index.bp.BPTree;
import com.WangTeng.MiniDB.meta.Attribute;
import com.WangTeng.MiniDB.meta.Table;
import com.WangTeng.MiniDB.meta.value.Value;


public class BasicGenTable {

    public Table genTable() {
        Table table = new Table();
        table.setName("test");
        table.setAttributes(getTableAttributes());
        BPTree clusterIndex = new BPTree(table, "clusterIndex", getClusterAttributes());
        clusterIndex.setPrimaryKey(true);
        table.setClusterIndex(clusterIndex);
        BPTree secondIndex = new BPTree(table, "secondIndex", getSecondAttributes());
        table.getSecondIndexes().add(secondIndex);
        return table;
    }

    public Attribute[] getTableAttributes() {
        Attribute[] attributes = new Attribute[3];
        // id 作为主键
        attributes[0] = new Attribute("id", Value.LONG, 0, "id");
        attributes[0].setPrimaryKey(true);
        attributes[1] = new Attribute("name", Value.STRING, 1, "name");
        attributes[2] = new Attribute("comment", Value.STRING, 2, "comment");
        return attributes;
    }

    public Attribute[] getClusterAttributes() {
        Attribute[] attributes = new Attribute[1];
        attributes[0] = new Attribute("id", Value.LONG, 0, "id");
        return attributes;
    }

    public Attribute[] getSecondAttributes() {
        Attribute[] attributes = new Attribute[2];
        attributes[0] = new Attribute("name", Value.STRING, 0, "name");
        attributes[1] = new Attribute("id", Value.LONG, 1, "id");
        return attributes;
    }
}
