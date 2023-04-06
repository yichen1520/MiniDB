package com.WangTeng.MiniDB.engine.net.proto.packet;

import java.util.ArrayList;
import java.util.List;

/**
 * 传输二进制的字段值(服务端/客户端)
 */
public class RowDataPacket extends MySQLPacket {
    private static final byte NULL_MARK = (byte) 251;

    public final int fieldCount;
    public final List<byte[]> fieldValues;
    public final List<String> fieldStrings;

    public RowDataPacket(int fieldCount) {
        this.fieldCount = fieldCount;
        this.fieldValues = new ArrayList<byte[]>(fieldCount);
        this.fieldStrings = new ArrayList<String>(fieldCount);
    }

    @Override
    public int calcPacketSize() {
        return 0;
    }

    @Override
    protected String getPacketInfo() {
        return null;
    }
}
