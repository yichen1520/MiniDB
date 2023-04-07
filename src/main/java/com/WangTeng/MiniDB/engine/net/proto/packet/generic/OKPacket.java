package com.WangTeng.MiniDB.engine.net.proto.packet.generic;

import com.WangTeng.MiniDB.engine.net.proto.MySQLMessage;
import com.WangTeng.MiniDB.engine.net.proto.packet.MySQLPacket;
import com.WangTeng.MiniDB.engine.net.proto.packet.BinaryPacket;
import com.WangTeng.MiniDB.engine.net.utils.BufferUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * OK数据包从服务器发送到客户端，以发出命令成功完成的信号。
 * OK数据包中的message信息包含消息的长度，确保执行结果的正确解析
 * 但和MySQL协议中OKPacket中info不同的是，这里message的长度单指message的长度值
 */
public class OKPacket extends MySQLPacket {
    public static final byte HEADER = 0x00;   // OK: header = 0 and length of packet > 7

    public static final byte[] OK = new byte[]{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};
    public static final byte[] AUTH_OK = new byte[]{7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0};

    public byte header = HEADER;
    public long affectedRows;   //int<lenenc>
    public long lastInsertId;   //int<lenenc>,值为AUTO_INCREMENT索引字段生成，如果没有索引字段，则为0x00
    public int status_flags;    //占用2个字节，服务器状态，客户端可以通过该值检查命令是否在事务处理中
    public int warnings;       //占用2个字节，告警次数，即告警发生的次数
    public byte[] message;


    public void read(BinaryPacket bin) {
        packetLength = bin.packetLength;
        packetId = bin.packetId;
        MySQLMessage mm = new MySQLMessage(bin.data);
        header = mm.read();
        affectedRows = mm.readLength();
        lastInsertId = mm.readLength();
        status_flags = mm.readUB2();
        warnings = mm.readUB2();
        if(mm.hasRemaining()){
            message = mm.readBytesWithLength();
        }
    }

    public void write(ChannelHandlerContext ctx){
        //默认使用直接内存，缓冲区大小为256字节
        ByteBuf buffer = ctx.alloc().buffer();

        BufferUtil.writeUB3(buffer,calcPacketSize());
        BufferUtil.writeByte(buffer,packetId);

        BufferUtil.writeByte(buffer,header);
        BufferUtil.writeLength(buffer,affectedRows);
        BufferUtil.writeLength(buffer,lastInsertId);
        BufferUtil.writeUB2(buffer,status_flags);
        BufferUtil.writeUB2(buffer,warnings);
        if(message != null){
            BufferUtil.writeWithLength(buffer,message); //写入长度 + message自身信息
        }
        ctx.writeAndFlush(buffer);
    }

    @Override
    public int calcPacketSize() {
        int i = 1;  //OKPacket的header
        i += BufferUtil.getLength(affectedRows);
        i += BufferUtil.getLength(lastInsertId);
        i += 4;     //status_flags 和 warnings 共占4个字节
        if(message != null){
            i += BufferUtil.getLength(message);
        }
        return i;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL OK Packet";
    }
}
