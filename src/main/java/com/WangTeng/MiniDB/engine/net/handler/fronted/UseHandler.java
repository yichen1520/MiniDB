package com.WangTeng.MiniDB.engine.net.handler.fronted;

public class UseHandler {

    public static void handle(String sql, FrontendConnection c, int offset) {
        // todo actual use
        c.writeOk();

    }
}
