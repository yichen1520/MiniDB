package com.WangTeng.MiniDB.engine.net.response;

import com.WangTeng.MiniDB.engine.net.handler.fronted.FrontendConnection;
import com.WangTeng.MiniDB.engine.net.proto.packet.generic.EOFPacket;
import com.WangTeng.MiniDB.engine.net.proto.packet.other.FieldPacket;
import com.WangTeng.MiniDB.engine.net.proto.packet.other.ResultSetHeaderPacket;
import com.WangTeng.MiniDB.engine.net.proto.packet.other.RowDataPacket;
import com.WangTeng.MiniDB.engine.net.proto.utils.Fields;
import com.WangTeng.MiniDB.engine.net.proto.utils.PacketUtil;
import com.WangTeng.MiniDB.engine.net.proto.utils.StringUtil;
import com.WangTeng.MiniDB.meta.TableManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;


import java.util.ArrayList;
import java.util.List;

public class ShowTables {
    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("TABLES", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void response(FrontendConnection c) {
        ChannelHandlerContext ctx = c.getCtx();
        ByteBuf buffer = ctx.alloc().buffer();

        // write header
        buffer = header.writeBuf(buffer, ctx);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.writeBuf(buffer, ctx);
        }

        // write eof
        buffer = eof.writeBuf(buffer, ctx);

        // write rows
        byte packetId = eof.packetId;

        for (String name : getTables()) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(name, c.getCharset()));
            row.packetId = ++packetId;
            buffer = row.writeBuf(buffer, ctx);
        }

        // write lastEof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.writeBuf(buffer, ctx);

        // write buffer
        ctx.writeAndFlush(buffer);
    }

    private static List<String> getTables() {
        ArrayList<String> list = new ArrayList<>();
        for (String tableName : TableManager.tableMap.keySet()) {
            list.add(tableName);
        }
        return list;
    }
}
