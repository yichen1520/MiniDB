package com.WangTeng.MiniDB.access;

import com.WangTeng.MiniDB.meta.IndexEntry;

public interface Cursor {

    IndexEntry next();

    void reset();
}
