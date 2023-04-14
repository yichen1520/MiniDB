package com.WangTeng.MiniDB.store.log;

import com.WangTeng.MiniDB.config.SystemConfig;
import com.WangTeng.MiniDB.meta.IndexEntry;
import com.WangTeng.MiniDB.store.fs.FileUtils;
import com.WangTeng.MiniDB.transaction.OpType;
import com.WangTeng.MiniDB.transaction.log.Log;
import com.WangTeng.MiniDB.transaction.log.LogType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * 日志存储需要处理大量的写入操作
 * 不使用池化的ByteBuf对象，避免对象池的管理开销过大，影响写入性能
 *  也可以更好地适应高并发的写入场景
 */
public class LogStore {
    // 文件路径
    private String logPath;
    // 文件channel
    private FileChannel fileChannel;

    private ByteBufAllocator byteBufAllocator;

    public LogStore() {
        this.logPath = SystemConfig.MINIDB_LOG_FILE_NAME;
        // 不使用池化的ByteBuf对象，每次调用buffer()都会创建一个新的对象
        byteBufAllocator = new UnpooledByteBufAllocator(false);
        open();
    }

    public void open() {
        fileChannel = FileUtils.open(logPath);
    }

    public void close() {
        FileUtils.closeFile(fileChannel);
    }

    public void appendLog(Log log) {
        ByteBuf byteBuf = byteBufAllocator.buffer(1024);
        log.writeBytes(byteBuf);
        append(byteBuf.nioBuffer());
    }

    // for 重新启动时候使用
    public List<Log> loadLog() {
        // 从文件开始load
        try {
            fileChannel.position(0);
            long length = fileChannel.size();
            ByteBuffer byteBuffer = ByteBuffer.allocate((int) length);
            FileUtils.readFully(fileChannel, byteBuffer);
            return readAllLog(byteBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Log> readAllLog(ByteBuffer byteBuffer) {
        List<Log> logs = new ArrayList<>();
        ByteBuf byteBuf = byteBufAllocator.buffer(byteBuffer.capacity());
        byteBuf.writeBytes(byteBuffer.array());

        while (byteBuf.readableBytes() > 0) {
            Log log = new Log();
            // for lsn
            log.setLsn(byteBuf.readLong());
            // logType
            log.setLogType(byteBuf.readInt());
            // trxId
            log.setTrxId(byteBuf.readInt());
            // 只有row的日志才有记录,其它只是记录了其标识
            if (log.getLogType() == LogType.ROW) {
                int tableNameLength = byteBuf.readInt();
                byte[] byteName = new byte[tableNameLength];
                byteBuf.readBytes(byteName);
                String tableName = new String(byteName);
                log.setTableName(tableName);
                log.setOpType(byteBuf.readInt());
                int length = byteBuf.readInt();
                byte[] entryByte = new byte[length];
                byteBuf.readBytes(entryByte);
                IndexEntry entry = new IndexEntry();
                entry.read(entryByte);
                if (log.getOpType() == OpType.insert) {
                    log.setAfter(entry);
                } else if (log.getOpType() == OpType.delete) {
                    log.setAfter(entry);
                }
            }
            logs.add(log);
        }
        return logs;
    }

    // 添加日志
    public void append(ByteBuffer dst) {
        try {
            FileUtils.append(fileChannel, dst);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
