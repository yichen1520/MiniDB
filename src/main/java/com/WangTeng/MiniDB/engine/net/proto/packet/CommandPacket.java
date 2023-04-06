package com.WangTeng.MiniDB.engine.net.proto.packet;

/**
 *
 */
public class CommandPacket extends MySQLPacket {

    public byte command;
    public byte[] arg;

    @Override
    public int calcPacketSize() {
        return 0;
    }

    @Override
    protected String getPacketInfo() {
        return null;
    }
}
