package com.WangTeng.MiniDB.meta;

import java.util.HashMap;
import java.util.Map;

/**
 * 元组的属性描述
 */
public class IndexDesc {
    // 元组的属性数组 即创建索引的列名和数据类型
    // 属性中的index用于指向当前属性在元组的位置
    private Attribute[] attrs;
    // 主键属性
    private Attribute primaryAttr;

    // key:属性名称 value:属性
    private Map<String, Attribute> attrsMap = new HashMap<>();

    public IndexDesc(Attribute[] attrs) {
        this.attrs = attrs;
        attrsMap = new HashMap<>();
        for (Attribute attr : attrs) {
            attrsMap.put(attr.getName(), attr);
            if (attr.isPrimaryKey()) {
                primaryAttr = attr;
            }
        }
    }

    public Attribute getPrimaryAttr() {
        return primaryAttr;
    }

    public void setPrimaryAttr(Attribute primaryAttr) {
        this.primaryAttr = primaryAttr;
    }

    public Attribute[] getAttrs() {
        return attrs;
    }

    public void setAttrs(Attribute[] attrs) {
        this.attrs = attrs;
    }

    public Map<String, Attribute> getAttrsMap() {
        return attrsMap;
    }

    public void setAttrsMap(Map<String, Attribute> attrsMap) {
        this.attrsMap = attrsMap;
    }
}
