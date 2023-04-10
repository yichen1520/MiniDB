package com.WangTeng.MiniDB.index.bp;

import com.WangTeng.MiniDB.config.SystemConfig;
import com.WangTeng.MiniDB.constant.ItemConst;
import com.WangTeng.MiniDB.meta.IndexEntry;
import com.WangTeng.MiniDB.meta.value.Value;
import com.WangTeng.MiniDB.meta.value.ValueInt;
import com.WangTeng.MiniDB.store.item.Item;
import com.WangTeng.MiniDB.store.page.Page;
import com.WangTeng.MiniDB.store.page.PageHeaderData;
import com.WangTeng.MiniDB.store.page.PageLoader;

/**
 * Page 时刻确保当前页的空间利用率 > 50%，避免产生大量的碎片空间
 * byte struct
 * PageHeaderData
 * isLeaf int
 * isRoot int
 * this pageNo int
 * parent pageNo int
 * entryCount int
 * entries Tuples
 * childCount int
 * childCounts Tuples
 * previous page
 * next page
 */
public class BpPage extends Page {
    private BPNode bpNode;

    // 叶子节点的初始自由空间大小
    private int leafInitFreeSpace;

    // 非叶子节点的初始自由空间大小
    private int nodeInitFreeSpace;

    public BpPage(int defaultSize) {
        super(defaultSize);
        init();
    }

    public BpPage(BPNode bpNode) {
        super(SystemConfig.DEFAULT_PAGE_SIZE);
        this.bpNode = bpNode;
        init();
    }

    /**
     * leaf节点需要存储数据和指向下一个节点(指向相邻节点)的指针，因此需要预留更多的空间
     * node节点只需要存储指向下一级节点的指针，因此需要预留较少的空间
     */
    private void init() {
        //leaf节点指的是B+树的最底层节点，也就是存储实际数据的节点
        leafInitFreeSpace =
                length - SystemConfig.DEFAULT_SPECIAL_POINT_LENGTH - ItemConst.INT_LENGTH * 7 - PageHeaderData
                        .PAGE_HEADER_SIZE;
        //node节点则指的是非叶子节点，它用来存储索引信息，指向下一级节点
        nodeInitFreeSpace =
                length - SystemConfig.DEFAULT_SPECIAL_POINT_LENGTH - ItemConst.INT_LENGTH * 6 - PageHeaderData
                        .PAGE_HEADER_SIZE;
    }

    /**
     * 从Page对象中读取数据并将其存储到BPNode对象中
     * 初始化该对象的一些属性，并建立与其它BPNode之间的联系，生成树形结构
     */
    public BPNode readFromPage(BPTree bpTree) {
        PageLoader loader = new PageLoader(this);
        loader.load();

        boolean isLeaf = getTupleBoolean(loader.getIndexEntries()[0]);
        boolean isRoot = getTupleBoolean(loader.getIndexEntries()[1]);

        bpNode = new BPNode(isLeaf, isRoot, bpTree);
        if (loader.getIndexEntries() == null) {
            // 处理没有记录的情况
            return bpNode;
        }
        // 由于是从磁盘中读取,以磁盘记录的为准
        int pageNo = getTupleInt(loader.getIndexEntries()[2]);
        bpNode.setPageNo(pageNo);

        // 首先在这边放入nodeMap,否则由于一直递归,一直没机会放入,导致循环递归
        bpTree.nodeMap.put(pageNo, bpNode);
        int parentPageNo = getTupleInt(loader.getIndexEntries()[3]);
        bpNode.setParent(bpTree.getNodeFromPageNo(parentPageNo));
        int entryCount = getTupleInt(loader.getIndexEntries()[4]);
        for (int i = 0; i < entryCount; i++) {
            bpNode.getEntries().add(loader.getIndexEntries()[5 + i]);
        }
        if (!isLeaf) {
            int childCount = getTupleInt(loader.getIndexEntries()[5 + entryCount]);
            int initSize = 6 + entryCount;
            for (int i = 0; i < childCount; i++) {
                int childPageNo = getTupleInt(loader.getIndexEntries()[initSize + i]);
                bpNode.getChildren().add(bpTree.getNodeFromPageNo(childPageNo));
            }
        } else {
            int initSize = 5 + entryCount;
            int previousNo = getTupleInt(loader.getIndexEntries()[initSize]);
            int nextNo = getTupleInt(loader.getIndexEntries()[initSize + 1]);
            bpNode.setPrevious(bpTree.getNodeFromPageNo(previousNo));
            bpNode.setNext(bpTree.getNodeFromPageNo(nextNo));
        }

        return bpNode;
    }

    /**
     * 与PageLoader联系了起来
     */
    public void writeToPage() {
        // header already write
        // write isLeaf
        writeTuple(genIsLeafTuple());
        // write isRoot
        writeTuple(genIsRootTuple());
        // this pageNo
        writeTuple(genTupleInt(bpNode.getPageNo()));
        // parent node pageNo
        if (!bpNode.isRoot) {
            writeTuple(genTupleInt(bpNode.getParent().getPageNo()));
        } else {
            // parent node -1 表示当前页面是root页面
            writeTuple(genTupleInt(-1));
        }
        // write entries,(count,entry1,entry2 ...)
        // entry count
        writeTuple(genTupleInt(bpNode.getEntries().size()));
        // entries
        for (int i = 0; i < bpNode.getEntries().size(); i++) {
            writeTuple(bpNode.getEntries().get(i));
        }
        if (!bpNode.isLeaf()) {
            // 非叶子节点
            // write childrenPageNo,(count,child1PageNo1,child2PageNo2 ...)
            writeTuple(genTupleInt(bpNode.getChildren().size()));
            for (int i = 0; i < bpNode.getChildren().size(); i++) {
                writeTuple(genTupleInt(bpNode.getChildren().get(i).getPageNo()));
            }
        } else {
            if (bpNode.getPrevious() == null) {
                writeTuple(genTupleInt(-1));
            } else {
                writeTuple(genTupleInt(bpNode.getPrevious().getPageNo()));
            }
            if (bpNode.getNext() == null) {
                writeTuple(genTupleInt(-1));
            } else {
                writeTuple(genTupleInt(bpNode.getNext().getPageNo()));
            }

        }
    }

    /**
     * 首先遍历节点中的所有索引条目，计算它们的长度并将其累加到size中。
     * 然后，如果该节点不是叶子节点，则遍历其所有子节点，将每个子节点的指针长度（即ItemConst.INT_LENGTH）也累加到size中
     * @return 返回BpPage节点中所有条目占用的字节数
     */
    public int getContentSize() {
        int size = 0;
        //计算当前页中根节点的索引条目大小
        for (IndexEntry key : bpNode.getEntries()) {
            size += Item.getItemLength(key);
        }
        if (!bpNode.isLeaf()) {
            for (int i = 0; i < bpNode.getChildren().size(); i++) {
                size += ItemConst.INT_LENGTH;
            }
        }
        return size;
    }

    public int cacluateRemainFreeSpace() {
        return getInitFreeSpace() - getContentSize();
    }

    public int getInitFreeSpace() {
        if (bpNode.isLeaf()) {
            return leafInitFreeSpace;
        } else {
            return nodeInitFreeSpace;
        }
    }

    private IndexEntry genIsLeafTuple() {
        return genBoolTuple(bpNode.isLeaf());
    }

    private IndexEntry genIsRootTuple() {
        return genBoolTuple(bpNode.isRoot());
    }

    // 当前bool tuple都用int代替
    private IndexEntry genBoolTuple(boolean b) {
        if (b) {
            return genTupleInt(1);
        } else {
            return genTupleInt(0);
        }
    }

    public static IndexEntry genTupleInt(int i) {
        Value[] vs = new Value[1];
        ValueInt valueInt = new ValueInt(i);
        vs[0] = valueInt;
        return new IndexEntry(vs);
    }

    public int getTupleInt(IndexEntry indexEntry) {
        return ((ValueInt) indexEntry.getValues()[0]).getInt();
    }

    public boolean getTupleBoolean(IndexEntry indexEntry) {
        int i = ((ValueInt) indexEntry.getValues()[0]).getInt();
        if (i == 1) {
            return true;
        } else {
            return false;
        }
    }
}
