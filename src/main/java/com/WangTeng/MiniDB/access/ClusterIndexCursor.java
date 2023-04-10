package com.WangTeng.MiniDB.access;

import com.WangTeng.MiniDB.index.bp.Position;

public class ClusterIndexCursor extends BaseIndexCursor {

    public ClusterIndexCursor(Position startPos, Position endPos, boolean isEqual) {
        super(startPos, endPos, isEqual);
    }
}
