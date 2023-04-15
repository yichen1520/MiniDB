package com.WangTeng.MiniDB.engine.net.handler.fronted;

import com.WangTeng.MiniDB.engine.net.proto.packet.generic.OKPacket;
import com.WangTeng.MiniDB.engine.net.proto.utils.ErrorCode;
import com.WangTeng.MiniDB.engine.net.proto.utils.StringUtil;
import io.netty.channel.ChannelHandlerContext;

public class KillHandler {

    public static void handle(String stmt, int offset, FrontendConnection c) {
        ChannelHandlerContext ctx = c.getCtx();
        String id = stmt.substring(offset).trim();
        if (StringUtil.isEmpty(id)) {
            c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "NULL connection id");
        } else {
            // get value
            long value = 0;
            try {
                value = Long.parseLong(id);
            } catch (NumberFormatException e) {
                c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Invalid connection id:" + id);
                return;
            }

            // kill myself
            if (value == c.getId()) {
                getOkPacket().write(ctx);
                return;
            }

            // get connection and close it
            FrontendConnection fc = FrontendGroupHandler.frontendGroup.get(value);

            if (fc != null) {
                fc.close();
                getOkPacket().write(ctx);
            } else {
                c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Unknown connection id:" + id);
            }
        }
    }

    private static OKPacket getOkPacket() {
        OKPacket packet = new OKPacket();
        packet.packetId = 1;
        packet.affectedRows = 0;
        packet.status_flags = 2;
        return packet;
    }

}
