package com.WangTeng.MiniDB.engine.net.response;

import com.WangTeng.MiniDB.engine.net.handler.fronted.FrontendConnection;
import com.WangTeng.MiniDB.engine.net.proto.packet.generic.ErrorPacket;
import org.apache.commons.lang.StringUtils;

public class ErrResponse {
    public static void response(FrontendConnection connection, String errMsg) {
        if (StringUtils.isNotEmpty(errMsg)) {
            ErrorPacket errorPacket = new ErrorPacket();
            errorPacket.message = errMsg.getBytes();
            errorPacket.write(connection.getCtx());
        }
    }
}
