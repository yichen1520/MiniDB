package com.WangTeng.MiniDB.engine.net.codec;

import java.util.List;

import com.WangTeng.MiniDB.engine.net.proto.packet.BinaryPacket;
import com.WangTeng.MiniDB.engine.net.proto.utils.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class MySqlPacketDecoder extends ByteToMessageDecoder {
    private static final Logger logger = LoggerFactory.getLogger(MySqlPacketDecoder.class);
    private final int packetHeaderSize = 4;                 //4个字节的消息头：3个字节的消息长度 + 1个字节的序号packetId
    private final int maxPacketSize = 16 * 1024 * 1024;     //发送的数据包最大为2^24,16M

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < packetHeaderSize) {
            return;
        }
        in.markReaderIndex();
        int packetLength = ByteUtil.readUB3(in);
        // 过载保护
        if (packetLength > maxPacketSize) {
            throw new IllegalArgumentException("Packet size over the limit " + maxPacketSize);
        }
        byte packetId = in.readByte();
        if (in.readableBytes() < packetLength) {
            // 半包回溯，读指针重置回到标记位置
            in.resetReaderIndex();
            return;
        }
        //将数据包读取为BinaryPacket
        BinaryPacket packet = new BinaryPacket();
        packet.packetLength = packetLength;
        packet.packetId = packetId;
        // data will not be accessed any more,so we can use this array safely
        packet.data = in.readBytes(packetLength).array();
        if (packet.data == null || packet.data.length == 0) {
            logger.error("getDecoder data errorMessage,packetLength=" + packet.packetLength);
        }
        out.add(packet);
    }
}
