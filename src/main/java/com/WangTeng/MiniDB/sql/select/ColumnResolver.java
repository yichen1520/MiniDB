package com.WangTeng.MiniDB.sql.select;

import com.WangTeng.MiniDB.meta.Attribute;
import com.WangTeng.MiniDB.meta.value.Value;

public interface ColumnResolver {

    Attribute[] getAttributes();

    Value getValue(String columnName);

    TableFilter getTableFilter();

    String getTableAlias();
}
