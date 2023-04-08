package com.WangTeng.MiniDB.engine.net.handler.factory;

import com.WangTeng.MiniDB.engine.net.handler.fronted.FrontendConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class FrontConnectionFactory {
    private static final Logger logger = LoggerFactory.getLogger(FrontConnectionFactory.class);

    //MySQL ThreadId自增器
    private static final AtomicInteger ACCEPT_SEQ = new AtomicInteger(0);

    public FrontendConnection getConnection(){
        FrontendConnection connection = new FrontendConnection();

        return connection;
    }
}
