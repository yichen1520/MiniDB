package com.WangTeng.MiniDB.engine.net.handler.fronted;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 前端连接收集器
 */
public class FrontendGroupHandler extends ChannelHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(FrontendGroupHandler.class);

    public static ConcurrentHashMap<Long, FrontendConnection> frontendGroup = new ConcurrentHashMap<>();

    protected FrontendConnection source;

    public FrontendGroupHandler(FrontendConnection source) {
        this.source = source;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        frontendGroup.put(source.getId(), source);
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        frontendGroup.remove(source.getId());
        source.close();
        ctx.fireChannelActive();
    }

}
