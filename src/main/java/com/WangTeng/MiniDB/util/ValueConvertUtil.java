package com.WangTeng.MiniDB.util;

import com.WangTeng.MiniDB.meta.Attribute;
import com.WangTeng.MiniDB.meta.value.Value;
import com.WangTeng.MiniDB.meta.value.ValueInt;
import com.WangTeng.MiniDB.meta.value.ValueString;

import java.util.ArrayList;
import java.util.List;

/**
 * Value和Attribute两者之间相互转换的工具类
 */
public class ValueConvertUtil {
    public static Value[] convertAttr(Attribute attr) {
        List<Value> list = new ArrayList<>();
        list.add(new ValueString(attr.getName()));
        list.add(new ValueInt(attr.getType()));
        list.add(new ValueInt(attr.getIndex()));
        list.add(new ValueString(attr.getComment()));
        list.add(new ValueInt(attr.isPrimaryKey() ? 1 : 0));
        return list.toArray(new Value[list.size()]);
    }

    public static Attribute convertValue(Value[] values) {
        Attribute attr = new Attribute();
        attr.setName(values[0].getString());
        attr.setType(values[1].getInt());
        attr.setIndex(values[2].getInt());
        attr.setComment(values[3].getString());
        attr.setPrimaryKey(values[4].getInt() > 0 ? true : false);
        return attr;
    }
}
