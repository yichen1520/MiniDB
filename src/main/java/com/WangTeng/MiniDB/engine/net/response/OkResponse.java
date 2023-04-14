package com.WangTeng.MiniDB.engine.net.response;

import com.WangTeng.MiniDB.engine.net.handler.fronted.FrontendConnection;
import com.WangTeng.MiniDB.engine.net.proto.packet.generic.OKPacket;
import io.netty.channel.ChannelHandlerContext;

public class OkResponse {
    public static void response(FrontendConnection c) {
        OKPacket okPacket = new OKPacket();
        ChannelHandlerContext ctx = c.getCtx();
        okPacket.write(ctx);
    }

    public static void responseWithAffectedRows(FrontendConnection c, long affectedRows) {
        OKPacket okPacket = new OKPacket();
        okPacket.affectedRows = affectedRows;
        ChannelHandlerContext ctx = c.getCtx();
        okPacket.write(ctx);
    }
}
