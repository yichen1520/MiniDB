package com.WangTeng.MiniDB.engine.net.proto.packet;

/**
 * ResultSet类报文之一，包含列数量和一些额外的信息
 */
public class ResultSetHeaderPacket extends MySQLPacket {
    public int fieldCount;
    public long extra;

    @Override
    public int calcPacketSize() {
        return 0;
    }

    @Override
    protected String getPacketInfo() {
        return null;
    }
}
