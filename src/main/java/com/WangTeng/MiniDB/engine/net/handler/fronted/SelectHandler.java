package com.WangTeng.MiniDB.engine.net.handler.fronted;

import com.WangTeng.MiniDB.engine.net.response.SelectDatabase;
import com.WangTeng.MiniDB.engine.net.response.SelectVersion;
import com.WangTeng.MiniDB.engine.net.response.SelectVersionComment;
import com.WangTeng.MiniDB.engine.net.response.jdbc.SelectIncrementResponse;
import com.WangTeng.MiniDB.engine.parser.ServerParse;
import com.WangTeng.MiniDB.engine.parser.ServerParseSelect;

public class SelectHandler {

    private static String selectIncrement = "SELECT @@session.auto_increment_increment";

    public static void handle(String stmt, FrontendConnection c, int offs) {
        int offset = offs;
        switch (ServerParseSelect.parse(stmt, offs)) {
            case ServerParseSelect.DATABASE:
                SelectDatabase.response(c);
                break;
            case ServerParseSelect.VERSION_COMMENT:
                SelectVersionComment.response(c);
                break;
            case ServerParseSelect.VERSION:
                SelectVersion.response(c);
                break;
            default:
                if (selectIncrement.equals(stmt)) {
                    SelectIncrementResponse.response(c);
                } else {
                    c.execute(stmt, ServerParse.SELECT);
                }
                break;
        }
    }

}
