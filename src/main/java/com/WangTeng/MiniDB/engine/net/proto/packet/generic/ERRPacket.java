package com.WangTeng.MiniDB.engine.net.proto.packet.generic;

import com.WangTeng.MiniDB.engine.net.proto.packet.MySQLPacket;

public class ERRPacket extends MySQLPacket {
    public static final byte HEADER = (byte) 0xFF;
    public static final byte SQL_STATE_MARKER = (byte) '#'; //服务器状态标志
    public static final byte[] DEFAULT_SQL_STATE = "HY000".getBytes();

    public byte header = HEADER;
    public int errorCode;
    public byte sqlStateMarker = SQL_STATE_MARKER;
    public byte[] sqlState = DEFAULT_SQL_STATE;
    public byte[] message;

    @Override
    public int calcPacketSize() {
        return 0;
    }

    @Override
    protected String getPacketInfo() {
        return null;
    }
}
