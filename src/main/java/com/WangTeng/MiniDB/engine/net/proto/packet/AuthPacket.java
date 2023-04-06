package com.WangTeng.MiniDB.engine.net.proto.packet;

/**
 * 客户端向服务端发送的认证报文
 */
public class AuthPacket extends MySQLPacket {
    private static final byte[] FILLER = new byte[23];

    public long clientserverCapabilities;   //客户端权能
    public long maxPacketSize;
    public int charsetIndex;
    public String user;
    public byte[] password;
    public String database;
    public byte[] extra;// from FILLER(23)

    @Override
    public int calcPacketSize() {
        return 0;
    }

    @Override
    protected String getPacketInfo() {
        return null;
    }
}
