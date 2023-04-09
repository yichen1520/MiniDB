package com.WangTeng.MiniDB.store.fs;

import com.WangTeng.MiniDB.config.SystemConfig;
import com.WangTeng.MiniDB.index.bp.BpPage;
import com.WangTeng.MiniDB.store.page.Page;
import com.WangTeng.MiniDB.store.page.PageLoader;
import com.WangTeng.MiniDB.store.page.PagePool;
import com.WangTeng.MiniDB.transaction.log.Log;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 文件读写
 */
public class FStore {
    // 文件路径
    private String filePath;
    // 文件channel
    private FileChannel fileChannel;
    // 当前filePosition
    private long currentFilePosition;

    public FStore(String filePath) {
        this.filePath = filePath;
        currentFilePosition = 0;
        open();
    }


    public void open() {
        fileChannel = FileUtils.open(filePath);
    }

    public Page readPageFromFile(int pageIndex) {
        return readPageFromFile(pageIndex, false);
    }

    public Page readPageFromFile(int pageIndex, boolean isIndex) {
        int readPos = pageIndex * SystemConfig.DEFAULT_PAGE_SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(SystemConfig.DEFAULT_PAGE_SIZE);
        try {
            FileUtils.readFully(fileChannel, buffer, readPos);
        } catch (EOFException e) {
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // byteBuffer 转 buffer
        byte[] b = new byte[SystemConfig.DEFAULT_PAGE_SIZE];
        // position跳回原始位置 切换到读指针
        buffer.flip();
        buffer.get(b);
        if (!isIndex) {
            // 从池中拿取空页
            Page page = PagePool.getIntance().getFreePage();
            // 初始化page
            page.read(b);
            return page;
        } else {
            BpPage bpPage = new BpPage(SystemConfig.DEFAULT_PAGE_SIZE);
            bpPage.read(b);
            return bpPage;
        }
    }

    public PageLoader readPageLoaderFromFile(int pageIndex) {
        Page page = readPageFromFile(pageIndex);
        PageLoader loader = new PageLoader(page);
        // 装载byte
        loader.load();
        return loader;
    }

    public void writePageToFile(Page page, int pageIndex) {
        try {
            int writePos = pageIndex * SystemConfig.DEFAULT_PAGE_SIZE;
            ByteBuffer byteBuffer = ByteBuffer.wrap(page.getBuffer());
            FileUtils.writeFully(fileChannel, byteBuffer, writePos);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void append(Log log) {

    }

    public void close() {
        FileUtils.closeFile(fileChannel);
    }
}
