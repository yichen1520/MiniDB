package com.WangTeng.MiniDB.engine.net.proto.packet.other;

import com.WangTeng.MiniDB.engine.net.proto.MySQLMessage;
import com.WangTeng.MiniDB.engine.net.proto.packet.BinaryPacket;
import com.WangTeng.MiniDB.engine.net.proto.packet.MySQLPacket;
import com.WangTeng.MiniDB.engine.net.utils.BufferUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * 数据表的列信息
 */
public class FieldPacket extends MySQLPacket {
    private static final byte[] DEFAULT_CATALOG = "def".getBytes();
    private static final byte[] FILLER = new byte[2];

    public byte[] catalog = DEFAULT_CATALOG;
    public byte[] db;
    public byte[] table;
    public byte[] orgTable;
    public byte[] name;
    public byte[] orgName;
                                //1个字节 0x0c
    public int charsetIndex;    //2个字节，因为需要传递更详细的字符集信息
    public long length;         //4个字节
    public int type;            //1个字节
    public int flags;           //2个字节
    public byte decimals;       //1个字节
                                //填充2个字节
    public byte[] definition;

    public void read(byte[] data){
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        readBody(mm);
    }

    public void read(BinaryPacket bin){
        packetLength = bin.packetLength;
        packetId = bin.packetId;
        readBody(new MySQLMessage(bin.data));
    }

    private void readBody(MySQLMessage mm) {
        catalog = mm.readBytesWithLength();
        db = mm.readBytesWithLength();
        table = mm.readBytesWithLength();
        orgTable = mm.readBytesWithLength();
        name = mm.readBytesWithLength();
        orgName = mm.readBytesWithLength();
        mm.move(1);
        charsetIndex = mm.readUB2();
        length = mm.readUB4();
        type = mm.read() & 0xff;
        flags = mm.readUB2();
        decimals = mm.read();
        mm.move(FILLER.length);
        if(mm.hasRemaining()){
            definition = mm.readBytesWithLength();
        }
    }

    @Override
    public ByteBuf writeBuf(ByteBuf buffer, ChannelHandlerContext ctx) {
        int size = calcPacketSize();
        BufferUtil.writeUB3(buffer,size);
        BufferUtil.writeByte(buffer,packetId);
        writeBody(buffer);
        //ctx.writeAndFlush(buffer);
        return buffer;
    }

    private void writeBody(ByteBuf buffer) {
        byte nullVal = 0;
        BufferUtil.writeWithLength(buffer, catalog, nullVal);
        BufferUtil.writeWithLength(buffer, db, nullVal);
        BufferUtil.writeWithLength(buffer, table, nullVal);
        BufferUtil.writeWithLength(buffer, orgTable, nullVal);
        BufferUtil.writeWithLength(buffer, name, nullVal);
        BufferUtil.writeWithLength(buffer, orgName, nullVal);
        buffer.writeByte((byte) 0x0C);
        BufferUtil.writeUB2(buffer, charsetIndex);
        BufferUtil.writeUB4(buffer, length);
        buffer.writeByte((byte) (type & 0xff));
        BufferUtil.writeUB2(buffer, flags);
        buffer.writeByte(decimals);
        buffer.writeBytes(FILLER);
        if (definition != null) {
            BufferUtil.writeWithLength(buffer, definition);
        }
    }

    @Override
    public int calcPacketSize() {
        int size = (catalog == null) ? 1 : BufferUtil.getLength(catalog);
        size += (db == null) ? 1 : BufferUtil.getLength(db);
        size += (table == null) ? 1 : BufferUtil.getLength(table);
        size += (orgTable == null) ? 1 : BufferUtil.getLength(orgTable);
        size += (name == null) ? 1 : BufferUtil.getLength(name);
        size += (orgName == null) ? 1 : BufferUtil.getLength(orgName);
        size += 13;  // 1+2+4+1+2+1+2
        size += (definition == null) ? 1 : BufferUtil.getLength(definition);
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Field Packet";
    }
}
