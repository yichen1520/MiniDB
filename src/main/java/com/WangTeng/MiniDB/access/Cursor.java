package com.WangTeng.MiniDB.access;

import com.WangTeng.MiniDB.meta.IndexEntry;

/**
 * 光标
 */
public interface Cursor {

    IndexEntry next();

    void reset();
}
