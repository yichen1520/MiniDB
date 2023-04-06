package com.WangTeng.MiniDB.engine.net.proto.packet.generic;

import com.WangTeng.MiniDB.engine.net.proto.packet.MySQLPacket;

/**
 * 标记一个查询结果的结束.
 * 由于EOF值与其它Result Set结构共用1字节，所以在收到报文后需要对EOF包的真实性进行校验，校验条件为：
 *                 第1字节值为0xFE
 *                 包长度小于9字节
 */
public class EOFPacket extends MySQLPacket {
    public static final byte HEADER = (byte) 0xFE;   // OK: header = 0 and length of packet > 7

    public byte header = HEADER;
    public int status_flags;
    public int warinings;

    @Override
    public int calcPacketSize() {
        return 0;
    }

    @Override
    protected String getPacketInfo() {
        return null;
    }
}
