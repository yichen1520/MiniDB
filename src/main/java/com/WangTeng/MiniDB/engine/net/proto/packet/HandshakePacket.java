package com.WangTeng.MiniDB.engine.net.proto.packet;

/**
 * 初始化握手，登录认证交互报文
 */
public class HandshakePacket extends MySQLPacket {
    private static final byte[] FILLER_13 = new byte[13];   //填充

    public byte protocolVersion;
    public byte[] serverVersion;
    public long threadId;
    public byte[] seed;                 //挑战随机数，用于数据库验证
    public int serverCapabilities;      //服务权能，用于与客户端协商通讯方式
    public byte serverCharsetIndex;     //字符编码
    public int serverStatus;
    public byte[] restOfScrambleBuff;   //剩余加扰缓冲区(权能+密码)

    @Override
    public int calcPacketSize() {
        return 0;
    }

    @Override
    protected String getPacketInfo() {
        return null;
    }
}
