package com.WangTeng.MiniDB.engine.net.proto.packet.other;

import com.WangTeng.MiniDB.engine.net.proto.MySQLMessage;
import com.WangTeng.MiniDB.engine.net.proto.packet.BinaryPacket;
import com.WangTeng.MiniDB.engine.net.proto.packet.MySQLPacket;
import com.WangTeng.MiniDB.engine.net.proto.utils.BufferUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * 初始化握手，登录认证交互报文
 */
public class HandshakePacket extends MySQLPacket {
    private static final byte[] FILLER_13 = new byte[13];   //填充

    public byte protocolVersion;        //1个字节
    public byte[] serverVersion;
    public long threadId;               //4个字节
    public byte[] seed;                 //挑战随机数，用于数据库验证(rand1)
    public int serverCapabilities;      //2个字节 服务权能，用于与客户端协商通讯方式
    public byte serverCharsetIndex;     //1个字节 字符编码
    public int serverStatus;            //2个字节
    public byte[] restOfScrambleBuff;   //剩余加扰缓冲区(rand2)

    public void read(BinaryPacket bin) {
        packetLength = bin.packetLength;
        packetId = bin.packetId;
        MySQLMessage mm = new MySQLMessage(bin.data);
        protocolVersion = mm.read();
        serverVersion = mm.readBytesWithNull();
        threadId = mm.readUB4();
        seed = mm.readBytesWithNull();
        serverCapabilities = mm.readUB2();
        serverCharsetIndex = mm.read();
        serverStatus = mm.readUB2();
        mm.move(13);    //填充
        restOfScrambleBuff = mm.readBytesWithNull();
    }

    public void write(final ChannelHandlerContext ctx) {    //final修饰防止引用发生改变
        final ByteBuf buffer = ctx.alloc().buffer();
        BufferUtil.writeUB3(buffer, calcPacketSize());
        BufferUtil.writeByte(buffer, packetId);
        BufferUtil.writeByte(buffer, protocolVersion);
        BufferUtil.writeWithNull(buffer, serverVersion);
        BufferUtil.writeUB4(buffer, threadId);
        BufferUtil.writeWithNull(buffer, seed);
        BufferUtil.writeUB2(buffer, serverCapabilities);
        BufferUtil.writeByte(buffer, serverCharsetIndex);
        BufferUtil.writeUB2(buffer, serverStatus);
        BufferUtil.writeWithNull(buffer, restOfScrambleBuff);
        ctx.writeAndFlush(buffer);
    }

    @Override
    public int calcPacketSize() {
        int size = 1;   //协议版本号
        size += serverVersion.length;
        size += 5;      //1+4，1代表serverVersion结束后的0x00结尾占用一个字节；4代表threadId
        size += seed.length;
        size += 19;     //1+2+1+2+13
        size += restOfScrambleBuff.length;
        size += 1;       //restOfScrambleBuff结束后的0x00结尾占用一个字节
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Handshake Packet";
    }
}
