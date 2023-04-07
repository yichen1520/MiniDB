package com.WangTeng.MiniDB.engine.net.proto.packet.other;

import com.WangTeng.MiniDB.engine.net.proto.MySQLMessage;
import com.WangTeng.MiniDB.engine.net.proto.packet.BinaryPacket;
import com.WangTeng.MiniDB.engine.net.proto.packet.MySQLPacket;
import com.WangTeng.MiniDB.engine.net.proto.utils.BufferUtil;
import com.WangTeng.MiniDB.engine.net.proto.utils.constants.Capabilities;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

/**
 * 客户端向服务端发送的登录认证报文
 */
public class AuthPacket extends MySQLPacket {
    private static final byte[] FILLER = new byte[23];

    public long clientFlags;    //4个字节 客户端权能
    public long maxPacketSize;  //4个字节
    public int charsetIndex;    //1个字节
    public byte[] extra;        // from FILLER(23)
    public String user;
    public byte[] password;
    public String database;

    public void read(BinaryPacket bin) {
        packetLength = bin.packetLength;
        packetId = bin.packetId;
        MySQLMessage mm = new MySQLMessage(bin.data);
        clientFlags = mm.readUB4();
        maxPacketSize = mm.readUB4();
        charsetIndex = (mm.read() & 0xff);

        int current = mm.getPosition();
        int len = (int) mm.readLength();
        if (len > 0 && len < FILLER.length) {
            byte[] ab = new byte[len];
            System.arraycopy(mm.bytes(), mm.getPosition(), ab, 0, len);
            this.extra = ab;
        }
        mm.setPosition(current + FILLER.length);

        user = mm.readStringWithNull();
        password = mm.readBytesWithLength();
        if (((clientFlags & Capabilities.CLIENT_CONNECT_WITH_DB) != 0) && mm.hasRemaining()) {
            database = mm.readStringWithNull();
        }
    }

    public void write(ChannelHandlerContext ctx) {
        ByteBuf buffer = ctx.alloc().buffer();
        BufferUtil.writeUB3(buffer, calcPacketSize());
        BufferUtil.writeByte(buffer, packetId);
        BufferUtil.writeUB4(buffer, clientFlags);
        BufferUtil.writeUB4(buffer, maxPacketSize);
        BufferUtil.writeByte(buffer, (byte) charsetIndex);
        BufferUtil.writeBytes(buffer, FILLER);
        if (user == null) {
            BufferUtil.writeByte(buffer, (byte) 0);
        } else {
            BufferUtil.writeWithNull(buffer, user.getBytes());
        }
        if (password == null) {
            BufferUtil.writeByte(buffer, (byte) 0);
        } else {
            BufferUtil.writeWithLength(buffer, password);
        }
        if (database == null) {
            BufferUtil.writeByte(buffer, (byte) 0);
        } else {
            BufferUtil.writeWithNull(buffer, database.getBytes());
        }
        ctx.writeAndFlush(buffer);
    }

    public void write(Channel c) {
        ByteBuf buffer = c.alloc().buffer();
        BufferUtil.writeUB3(buffer, calcPacketSize());
        BufferUtil.writeByte(buffer, packetId);
        BufferUtil.writeUB4(buffer, clientFlags);
        BufferUtil.writeUB4(buffer, maxPacketSize);
        BufferUtil.writeByte(buffer, (byte) charsetIndex);
        BufferUtil.writeBytes(buffer, FILLER);
        if (user == null) {
            BufferUtil.writeByte(buffer, (byte) 0);
        } else {
            BufferUtil.writeWithNull(buffer, user.getBytes());
        }
        if (password == null) {
            BufferUtil.writeByte(buffer, (byte) 0);
        } else {
            BufferUtil.writeWithLength(buffer, password);
        }
        if (database == null) {
            BufferUtil.writeByte(buffer, (byte) 0);
        } else {
            BufferUtil.writeWithNull(buffer, database.getBytes());
        }
        c.writeAndFlush(buffer);
    }

    @Override
    public int calcPacketSize() {
        int size = 32;  //4+4+1+23
        size += (user == null) ? 1 : user.length() + 1;   //以0x00结尾
        size += (password == null) ? 1 : BufferUtil.getLength(password);
        size += (database == null) ? 1 : database.length() + 1;
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Authentication Packet";
    }
}
