package com.WangTeng.MiniDB.engine.net.handler.fronted;

import com.WangTeng.MiniDB.engine.net.response.ErrResponse;
import com.WangTeng.MiniDB.engine.net.response.ShowDatabases;
import com.WangTeng.MiniDB.engine.net.response.ShowTables;
import com.WangTeng.MiniDB.engine.parser.ServerParseShow;

public class ShowHandler {
    public static void handle(String stmt, FrontendConnection c, int offset) {
        switch (ServerParseShow.parse(stmt, offset)) {
            case ServerParseShow.DATABASES:
                ShowDatabases.response(c);
                break;
            case ServerParseShow.SHOWTABLES:
                ShowTables.response(c);
                break;
            default:
                ErrResponse.response(c, "not support this set param");
                break;
        }
    }
}
