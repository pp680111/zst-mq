package com.zst.mq.broker.transport;

import com.zst.mq.broker.core.ActionHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettyTransport {
    private NettyTransportProperties properties;
    private ActionHandler actionHandler;
    private EventLoopGroup ioEventLoopGroup;
    private EventLoopGroup bizEventLoopGroup;
    private Channel channel;

    public NettyTransport(NettyTransportProperties properties, ActionHandler actionHandler) {
        this.properties = properties;
        this.actionHandler = actionHandler;
    }

    public void start() {
        ioEventLoopGroup = new NioEventLoopGroup(properties.getIoWorkerNum());
        bizEventLoopGroup = new NioEventLoopGroup(properties.getBizWorkerNum());

        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap()
                    .group(ioEventLoopGroup, bizEventLoopGroup)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.SO_SNDBUF, 32 * 1024)
                    .childOption(ChannelOption.SO_RCVBUF, 32 * 1024)
                    .childOption(EpollChannelOption.SO_REUSEPORT, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.DEBUG))
                    .childHandler(new Initializer());

            channel = serverBootstrap.bind(properties.getPort()).sync().channel();
            channel.closeFuture().addListener(future -> {
                stop();
            });

            log.info("netty transport server started");
        } catch (Exception e) {
            log.error("start netty transport server error", e);
        }
    }

    public void stop() {
        if (bizEventLoopGroup != null && !bizEventLoopGroup.isShutdown()) {
            bizEventLoopGroup.shutdownGracefully();
            bizEventLoopGroup = null;
        }

        if (ioEventLoopGroup != null && !ioEventLoopGroup.isShutdown()) {
            ioEventLoopGroup.shutdownGracefully();
            ioEventLoopGroup = null;
        }

        channel.close();
    }

    private class Initializer extends ChannelInitializer {
        @Override
        protected void initChannel(Channel channel) throws Exception {
            log.info("initialize inbound channel, " + channel.remoteAddress().toString());

            channel.pipeline()
                    .addLast(new FrameDecoder())
                    .addLast(new FrameEncoder())
                    .addLast(new DataFrameHandler(actionHandler))
                    .addLast(new ClientCloseHandler());
        }
    }
}
