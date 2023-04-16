package com.WangTeng.MiniDB.engine.net.handler.factory;

import com.WangTeng.MiniDB.config.SystemConfig;
import com.WangTeng.MiniDB.engine.net.handler.fronted.FrontendConnection;
import com.WangTeng.MiniDB.engine.net.handler.fronted.ServerQueryHandler;
import com.WangTeng.MiniDB.engine.session.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class FrontConnectionFactory {
    private static final Logger logger = LoggerFactory.getLogger(FrontConnectionFactory.class);

    //MySQL ThreadId自增器
    private static final AtomicInteger ACCEPT_SEQ = new AtomicInteger(0);

    public FrontendConnection getConnection(){
        FrontendConnection connection = new FrontendConnection();
        connection.setQueryHandler(new ServerQueryHandler(connection));
        connection.setId(ACCEPT_SEQ.getAndIncrement());
        logger.info("connection Id=" + connection.getId());
        connection.setCharset(SystemConfig.DEFAULT_CHARSET);
        connection.setTxIsolation(SystemConfig.DEFAULT_TX_ISOLATION);
        connection.setLastActiveTime();
        connection.setSession(SessionFactory.newSession(connection));
        return connection;
    }
}
