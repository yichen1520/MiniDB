package com.WangTeng.MiniDB.engine.net.proto.packet.generic;

import com.WangTeng.MiniDB.engine.net.proto.MySQLMessage;
import com.WangTeng.MiniDB.engine.net.proto.packet.BinaryPacket;
import com.WangTeng.MiniDB.engine.net.proto.packet.MySQLPacket;
import com.WangTeng.MiniDB.engine.net.utils.BufferUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * 标记一个查询结果的结束.
 * 由于EOF值与其它Result Set结构共用1字节，所以在收到报文后需要对EOF包的真实性进行校验，校验条件为：
 * 第1字节值为0xFE
 * 包长度小于9字节
 */
public class EOFPacket extends MySQLPacket {
    public static final byte HEADER = (byte) 0xFE;   // OK: header = 0 and length of packet > 7

    public byte header = HEADER;    //1个字节
    public int warinings;           //2个字节
    public int status_flags;        //2个字节

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        header = mm.read();
        warinings = mm.readUB2();
        status_flags = mm.readUB2();
    }

    public void read(BinaryPacket bin) {
        packetLength = bin.packetLength;
        packetId = bin.packetId;
        MySQLMessage mm = new MySQLMessage(bin.data);
        header = mm.read();
        warinings = mm.readUB2();
        status_flags = mm.readUB2();
    }

    @Override
    public ByteBuf writeBuf(ByteBuf buffer, ChannelHandlerContext ctx) {
        int size = calcPacketSize();
        BufferUtil.writeUB3(buffer, size);
        BufferUtil.writeByte(buffer, packetId);

        BufferUtil.writeByte(buffer, header);
        BufferUtil.writeUB2(buffer, warinings);
        BufferUtil.writeUB2(buffer, status_flags);
        //ctx.writeAndFlush(buffer);
        return buffer;
    }

    @Override
    public int calcPacketSize() {
        return 5;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL EOF Packet";
    }
}
