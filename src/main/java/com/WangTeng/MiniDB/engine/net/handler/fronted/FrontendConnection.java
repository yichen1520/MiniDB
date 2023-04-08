package com.WangTeng.MiniDB.engine.net.handler.fronted;

import com.WangTeng.MiniDB.engine.net.proto.packet.BinaryPacket;
import com.WangTeng.MiniDB.engine.session.Session;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 前端连接
 */
public class FrontendConnection {
    public static final int packetHeaderSize = 4;
    private static final Logger logger = LoggerFactory.getLogger(FrontendConnection.class);

    protected long id;
    protected String user;
    protected String host;
    protected int port;
    protected String schema;
    protected String charset;
    protected int charsetIndex;
    protected FrontendQueryHandler queryHandler;
    protected ChannelHandlerContext ctx;

    private long lastInsertId;
    private long lastActiveTime;

    private Session session;

    private static final long AUTH_TIMEOUT = 15 * 1000L;

    private volatile int txIsolation;
    private volatile boolean autoCommit = true;

    //初始化DB的同时，绑定后端连接
    public void initDB(BinaryPacket bin){

    }
}
