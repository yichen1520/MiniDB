package com.WangTeng.MiniDB.engine.net.handler.factory;

import com.WangTeng.MiniDB.config.SystemConfig;
import com.WangTeng.MiniDB.engine.net.codec.MySqlPacketDecoder;
import com.WangTeng.MiniDB.engine.net.handler.fronted.FrontendAuthenticator;
import com.WangTeng.MiniDB.engine.net.handler.fronted.FrontendConnection;
import com.WangTeng.MiniDB.engine.net.handler.fronted.FrontendGroupHandler;
import com.WangTeng.MiniDB.engine.net.handler.fronted.FrontendTailHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

public class FrontHandlerFactory extends ChannelInitializer<SocketChannel> {
    private FrontConnectionFactory factory;

    public FrontHandlerFactory() {
        factory = new FrontConnectionFactory();
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {

        FrontendConnection source = factory.getConnection();
        FrontendGroupHandler groupHandler = new FrontendGroupHandler(source);
        // 认证成功后会将 FrontendAuthenticator 替换为 FrontendCommandHandler
        FrontendAuthenticator authHandler = new FrontendAuthenticator(source);
        FrontendTailHandler tailHandler = new FrontendTailHandler(source);
        // 心跳handler
        ch.pipeline().addLast(new IdleStateHandler(SystemConfig.IDLE_CHECK_INTERVAL, SystemConfig.IDLE_CHECK_INTERVAL, SystemConfig.IDLE_CHECK_INTERVAL));
        // decode mysql packet depend on it's length
        ch.pipeline().addLast(new MySqlPacketDecoder());
        ch.pipeline().addLast(groupHandler);
        ch.pipeline().addLast(authHandler);
        ch.pipeline().addLast(tailHandler);

    }
}
