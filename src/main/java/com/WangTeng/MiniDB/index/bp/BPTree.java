package com.WangTeng.MiniDB.index.bp;

import com.WangTeng.MiniDB.access.ClusterIndexCursor;
import com.WangTeng.MiniDB.access.Cursor;
import com.WangTeng.MiniDB.access.SecondIndexCursor;
import com.WangTeng.MiniDB.index.BaseIndex;
import com.WangTeng.MiniDB.index.CompareType;
import com.WangTeng.MiniDB.meta.Attribute;
import com.WangTeng.MiniDB.meta.IndexEntry;
import com.WangTeng.MiniDB.meta.Table;
import com.WangTeng.MiniDB.meta.value.ValueInt;
import com.WangTeng.MiniDB.store.item.Item;
import com.WangTeng.MiniDB.store.page.Page;
import com.WangTeng.MiniDB.store.page.PageLoader;
import com.WangTeng.MiniDB.store.page.PagePool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 用来维护索引和页号之间的关系
 * B+树的性质：
 * 1、每个节点（除了根节点）至少有m/2个entries，其中m是B+树中每个节点最多能够存储的entries个数。
 * 2、合并后的新节点中entries的个数不能超过节点的最大容量m。
 */
public class BPTree extends BaseIndex {
    /**
     * 根节点
     */
    protected BPNode root;

    /**
     * 叶子节点的链表头
     */
    protected BPNode head;

    /**
     *  存储页号与根节点的映射关系，每个页号只对应一个父节点,即根节点
     */
    protected Map<Integer, BPNode> nodeMap;

    public BPTree(Table table, String indexName, Attribute[] attributes) {
        super(table, indexName, attributes);
        root = new BPNode(true, true, this);
        head = root;
        nodeMap = new HashMap<>();
    }

    public void loadFromDisk() {
        int rootPageNo = getRootPageNoFromMeta();
        getNodeFromPageNo(rootPageNo);
    }

    public int getRootPageNoFromMeta() {
        PageLoader loader = new PageLoader(fStore.readPageFromFile(0));
        loader.load();
        return ((ValueInt) loader.getIndexEntries()[0].getValues()[0]).getInt();
    }

    /**
     * 根据页号拿到根节点，如果nodeMap中不存在，需要从磁盘中读入
     * @return 返回页号为pageNo的页的根节点
     */
    public BPNode getNodeFromPageNo(int pageNo) {
        if (pageNo == -1) {
            return null;
        }
        BPNode bpNode = nodeMap.get(pageNo);
        if (bpNode != null) {
            return bpNode;
        }
        //从磁盘中读入对应页并返回
        BpPage bpPage = (BpPage) fStore.readPageFromFile(pageNo, true);
        //调用方法更新维护nodeMap
        bpNode = bpPage.readFromPage(this);

        if (bpNode.isRoot()) {
            root = bpNode;
        }
        if (bpNode.isLeaf() && bpNode.getPrevious() == null) {
            head = bpNode;
        }
        return bpNode;
    }

    @Override
    public Cursor searchEqual(IndexEntry key) {
        Position startPos = getFirst(key, CompareType.EQUAL);
        if (startPos == null) {
            return null;
        }
        startPos.setSearchEntry(key);
        if (isPrimaryKey) {
            return new ClusterIndexCursor(startPos, null, true);
        } else {
            SecondIndexCursor cursor = new SecondIndexCursor(startPos, null, true);
            cursor.setClusterIndex(table.getClusterIndex());
            return cursor;
        }
    }

    @Override
    public Cursor searchRange(IndexEntry lowKey, IndexEntry upKey) {
        Position startPos = getFirst(lowKey, CompareType.LOW);
        if (startPos == null) {
            return null;
        }
        Position endPos = null;
        if (upKey != null) {
            startPos.setSearchEntry(lowKey);
            if (upKey != null) {
                endPos = getLast(upKey, CompareType.UP);
            }
            if (endPos != null) {
                endPos.setSearchEntry(upKey);
            }
        }
        if (isPrimaryKey) {
            return new ClusterIndexCursor(startPos, endPos, false);
        } else {
            SecondIndexCursor cursor = new SecondIndexCursor(startPos, endPos, false);
            cursor.setClusterIndex(table.getClusterIndex());
            return cursor;
        }
    }

    /**
     * 查询第一个符合的key
     */
    @Override
    public Position getFirst(IndexEntry outKey, int CompareType) {
        //根据当前索引是否为主键构建一个新的索引元组
        IndexEntry key = buildEntry(outKey);
        Position position = root.get(key.getCompareEntry(), CompareType);
        if (position == null) {
            return null;
        }
        // 由于存在key大量一样的情况,所以必须往前遍历,因为前面也可能有相同的key;
        //通过索引找到对应的BPNode，进而得到其前置节点
        BPNode bpNode = position.getBpNode().getPrevious();
        while (bpNode != null) {
            // 从后往前倒查找
            for (int i = bpNode.getEntries().size() - 1; i >= 0; i--) {
                IndexEntry item = bpNode.getEntries().get(i);
                if (item.compareIndex(key) == 0) {
                    position.setBpNode(bpNode);
                    position.setPosition(i);
                }
                if (!item.equals(key)) {
                    break;
                }
            }
            bpNode = bpNode.getPrevious();
        }
        return position;
    }

    @Override
    public Position getLast(IndexEntry outKey, int compareType) {
        IndexEntry key = buildEntry(outKey);
        Position position = root.get(key.getCompareEntry(), compareType);
        if (position == null) {
            return null;
        }
        // 由于存在key一样的情况,所以必须往后遍历,因为后面也可能有相同的key;
        BPNode bpNode = position.getBpNode().getNext();
        while (bpNode != null) {
            boolean notEqualFound = false;
            // 从前往后查找
            for (int i = 0; i < bpNode.entries.size(); i++) {
                IndexEntry item = bpNode.getEntries().get(i);
                if (item.compareIndex(key) == 0) {
                    position.setBpNode(bpNode);
                    position.setPosition(i);
                }
                if (!item.equals(key)) {
                    notEqualFound = true;
                    break;
                }
            }
            if (notEqualFound) {
                break;
            } else {
                bpNode = bpNode.getNext();
            }
        }
        return position;
    }

    // 遍历当前bpNode以及之后的node
    @Override
    public List<IndexEntry> getAll(IndexEntry key) {
        Position res = getFirst(key, CompareType.LOW);
        List<IndexEntry> list = new ArrayList<>();
        BPNode bpNode = res.getBpNode();
        BPNode initNode = res.getBpNode();
        while (bpNode != null) {
            for (IndexEntry indexEntry : bpNode.getEntries()) {
                if (indexEntry.compareIndex(key) == 0) {
                    list.add(indexEntry);
                } else {
                    // 这边对initNode做特殊处理的原因是
                    // 需要将计算出来的firstNode中的等值key加进来
                    if (initNode != bpNode) {
                        break;
                    }
                }
            }
            bpNode = bpNode.getNext();
        }
        return list;
    }

    public Map<Integer, BPNode> getNodeMap() {
        return nodeMap;
    }

    public BPTree setNodeMap(Map<Integer, BPNode> nodeMap) {
        this.nodeMap = nodeMap;
        return this;
    }

    public boolean innerRemove(IndexEntry key) {
        return root.remove(key, this);
    }

    /**
     * 删除所有匹配的索引元组
     * @param key
     * @return 返回删除的索引项数目count
     */
    @Override
    public int remove(IndexEntry key) {
        int count = 0;
        while (true) {
            if (!innerRemove(key)) {
                break;
            }
            count++;
        }
        return count;
    }

    @Override
    public boolean removeOne(IndexEntry entry) {
        IndexEntry matchIndexEntry = buildEntry(entry);
        return innerRemove(matchIndexEntry);
    }

    @Override
    public void insert(IndexEntry entry, boolean isUnique) {
        IndexEntry matchIndexEntry = buildEntry(entry);
        root.insert(matchIndexEntry, this, isUnique);
    }

    @Override
    public void delete(IndexEntry entry) {
        IndexEntry matchIndexEntry = buildEntry(entry);
        root.remove(matchIndexEntry, this);
    }

    @Override
    public void flushToDisk() {
        writeMetaPage();
        // 深度遍历
        root.flushToDisk(fStore);
    }

    // MetaPage for root page no
    public void writeMetaPage() {
        Page page = PagePool.getIntance().getFreePage();
        page.writeItem(new Item(BpPage.genTupleInt(root.getPageNo())));
        fStore.writePageToFile(page, 0);
    }

    public BPNode getRoot() {
        return root;
    }

    public BPTree setRoot(BPNode root) {
        this.root = root;
        return this;
    }

    public BPNode getHead() {
        return head;
    }

    public BPTree setHead(BPNode head) {
        this.head = head;
        return this;
    }
}
