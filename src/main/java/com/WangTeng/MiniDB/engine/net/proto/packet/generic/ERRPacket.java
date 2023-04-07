package com.WangTeng.MiniDB.engine.net.proto.packet.generic;

import com.WangTeng.MiniDB.engine.net.proto.MySQLMessage;
import com.WangTeng.MiniDB.engine.net.proto.packet.BinaryPacket;
import com.WangTeng.MiniDB.engine.net.proto.packet.MySQLPacket;
import com.WangTeng.MiniDB.engine.net.utils.BufferUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * Error数据包中的message信息不包含消息的长度，只需要返回错误信息即可，最重要的是错误码
 */
public class ERRPacket extends MySQLPacket {
    public static final byte HEADER = (byte) 0xFF;
    public static final byte SQL_STATE_MARKER = (byte) '#'; //服务器状态标志
    public static final byte[] DEFAULT_SQL_STATE = "HY000".getBytes();

    public byte header = HEADER;                    //1个字节
    public int errorCode;                           //2个字节
    public byte sqlStateMarker = SQL_STATE_MARKER;  //1个字节
    public byte[] sqlState = DEFAULT_SQL_STATE;     //5个字节
    public byte[] message;

    public void read(BinaryPacket bin) {
        packetLength = bin.packetLength;
        packetId = bin.packetId;
        MySQLMessage mm = new MySQLMessage(bin.data);
        header = mm.read();
        errorCode = mm.readUB2();
        //判断服务器状态标志是否合法
        if (mm.hasRemaining() && mm.get(mm.getPosition()) == SQL_STATE_MARKER) {
            mm.read();
            sqlState = mm.readBytes(5);
        }
        message = mm.readBytes();
    }

    /**
     * @param data 表示一个完整的消息报文
     */
    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        header = mm.read();
        errorCode = mm.readUB2();
        //判断服务器状态标志是否合法
        if (mm.hasRemaining() && mm.get(mm.getPosition()) == SQL_STATE_MARKER) {
            mm.read();
            sqlState = mm.readBytes(5);
        }
        message = mm.readBytes();
    }
    @Override
    public void write(ChannelHandlerContext ctx){
        int size = calcPacketSize();
        ByteBuf buffer = ctx.alloc().buffer();

        BufferUtil.writeUB3(buffer,size);
        BufferUtil.writeByte(buffer,packetId);

        BufferUtil.writeByte(buffer,header);
        BufferUtil.writeUB2(buffer,errorCode);
        BufferUtil.writeByte(buffer,sqlStateMarker);
        BufferUtil.writeBytes(buffer,sqlState);
        if(message != null){
            BufferUtil.writeBytes(buffer,message);
        }

        ctx.writeAndFlush(buffer);
    }
    @Override
    public int calcPacketSize() {
        int i = 1;
        i += 8;
        if (message != null) {
            i += message.length;
        }
        return i;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Error Packet";
    }
}
