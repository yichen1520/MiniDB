package com.WangTeng.MiniDB.access;

import com.WangTeng.MiniDB.index.bp.BPNode;
import com.WangTeng.MiniDB.index.bp.Position;
import com.WangTeng.MiniDB.meta.IndexEntry;

/**
 * 索引游标类
 */
public class BaseIndexCursor implements Cursor {
    // 开始位置
    private Position startPos;
    // 结束位置
    private Position endPos;
    // 当前位置
    private Position currentPos;

    // 标记当前读取的数据是否和查询条件完全匹配
    private boolean isEqual;

    public BaseIndexCursor(Position startPos, Position endPos, boolean isEqual) {
        this.startPos = startPos;
        this.endPos = endPos;
        this.isEqual = isEqual;
        resetCurrentPos(startPos);
    }

    // 用开始的pos初始化startPos
    private void resetCurrentPos(Position startPos) {
        currentPos = new Position(startPos.getBpNode(), startPos.getPosition());
    }

    @Override
    public IndexEntry next() {
        IndexEntry resultEntry = innerNext();
        if (isEqual) {
            if (startPos.getSearchEntry().compareIndex(resultEntry) != 0) {
                return null;
            }
        }
        return resultEntry;
    }

    public IndexEntry innerNext() {
        BPNode bpNode = currentPos.getBpNode();
        int currentPosition = currentPos.getPosition();
        // 超过了endPos,则返回null
        if (endPos != null) {
            if (bpNode.equals(endPos.getBpNode())) {
                if (currentPos.getPosition() > endPos.getPosition()) {
                    return null;
                }
            }
        }
        if (currentPosition < bpNode.getEntries().size()) {
            IndexEntry indexEntry = bpNode.getEntries().get(currentPosition);
            currentPos.incrPosition();
            return indexEntry;
        } else {
            // 跳入下一个bpNode
            bpNode = bpNode.getNext();
            if (bpNode == null) {
                return null;
            } else {
                if (endPos != null) {
                    // 如果超过了endPos的下一个块,则直接返回null
                    if (bpNode.equals(endPos.getBpNode().getNext())) {
                        return null;
                    }
                }
                currentPos = new Position(bpNode, 1);
                return bpNode.getEntries().get(0);
            }
        }
    }

    public void reset() {
        resetCurrentPos(startPos);
    }

    public Position getStartPos() {
        return startPos;
    }

    public void setStartPos(Position startPos) {
        this.startPos = startPos;
    }

    public Position getEndPos() {
        return endPos;
    }

    public void setEndPos(Position endPos) {
        this.endPos = endPos;
    }
}
