package com.WangTeng.MiniDB.engine.net.proto.packet.other;

import com.WangTeng.MiniDB.engine.net.proto.MySQLMessage;
import com.WangTeng.MiniDB.engine.net.proto.packet.MySQLPacket;
import com.WangTeng.MiniDB.engine.net.proto.utils.BufferUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 *
 */
public class CommandPacket extends MySQLPacket {

    public byte command;
    public byte[] arg;

    public CommandPacket(String query, byte commandType) {
        packetId = 0;
        command = commandType;
        arg = query.getBytes();
    }

    public CommandPacket(String query) {
        packetId = 0;
        command = MySQLPacket.COM_QUERY;
        arg = query.getBytes();
    }

    public void read(byte[] data){
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        command = mm.read();
        arg = mm.readBytes();
    }

    public void write(ChannelHandlerContext ctx){
        ByteBuf buffer = ctx.alloc().buffer();
        BufferUtil.writeUB3(buffer,calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeByte(command);
        buffer.writeBytes(arg);
        ctx.writeAndFlush(buffer);
    }
    @Override
    public int calcPacketSize() {
        return 1 + arg.length;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Command Packet";
    }
}
