package com.WangTeng.MiniDB.engine.net.proto.packet.other;

import com.WangTeng.MiniDB.engine.net.proto.MySQLMessage;
import com.WangTeng.MiniDB.engine.net.proto.packet.MySQLPacket;
import com.WangTeng.MiniDB.engine.net.utils.BufferUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * ResultSet类报文之一，包含列数量和一些额外的信息 的真实长度
 */
public class ResultSetHeaderPacket extends MySQLPacket {
    public int fieldCount;
    public long extra;

    public void read(byte[] data){
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        fieldCount = (int) mm.readLength();
        if(mm.hasRemaining()){
            extra = mm.readLength();
        }
    }

    @Override
    public ByteBuf writeBuf(ByteBuf buffer, ChannelHandlerContext ctx) {
        int size = calcPacketSize();
        BufferUtil.writeUB3(buffer,size);
        BufferUtil.writeByte(buffer,packetId);
        BufferUtil.writeLength(buffer,fieldCount);
        if(extra > 0){
            BufferUtil.writeLength(buffer,extra);
        }
        return buffer;
    }

    @Override
    public int calcPacketSize() {
        int size = BufferUtil.getLength(fieldCount);
        if(extra > 0){
            size += BufferUtil.getLength(extra);
        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL ResultSetHeader Packet";
    }
}
