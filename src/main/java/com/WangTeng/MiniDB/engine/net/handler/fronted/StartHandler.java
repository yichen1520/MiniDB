package com.WangTeng.MiniDB.engine.net.handler.fronted;

import com.WangTeng.MiniDB.engine.net.proto.utils.ErrorCode;
import com.WangTeng.MiniDB.engine.parser.ServerParse;
import com.WangTeng.MiniDB.engine.parser.ServerParseStart;

public class StartHandler {

    public static void handle(String stmt, FrontendConnection c, int offset) {
        switch (ServerParseStart.parse(stmt, offset)) {
            case ServerParseStart.TRANSACTION:
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported statement");
                break;
            default:
                // todo data source
                c.execute(stmt, ServerParse.START);
                break;
        }
    }
}
