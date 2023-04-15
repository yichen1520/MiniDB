package com.WangTeng.MiniDB.engine.net.handler.fronted;

import com.WangTeng.MiniDB.engine.net.proto.utils.ErrorCode;

public class SavepointHandler {

    public static void handle(String stmt, FrontendConnection c) {
        c.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported statement");
    }
}
