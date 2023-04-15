package com.WangTeng.MiniDB.engine.net.handler.fronted;

public class BeginHandler {

    public static void handle(String stmt, FrontendConnection c) {
        c.commit();
        c.writeOk();
    }
}
