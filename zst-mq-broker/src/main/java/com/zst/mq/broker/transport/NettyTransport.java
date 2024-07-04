package com.zst.mq.broker.transport;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettyTransport {
    private NettyTransportProperties properties;
    private EventLoopGroup ioEventLoopGroup;
    private EventLoopGroup bizEventLoopGroup;
    private Channel channel;
    public void start() {
        ioEventLoopGroup = new NioEventLoopGroup(properties.getIoWorker());
        bizEventLoopGroup = new NioEventLoopGroup(properties.getBizWorker());

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
                    .childHandler(new Initializer());

            channel = serverBootstrap.bind(properties.getPort()).channel();
            channel.closeFuture().addListener(future -> {
                stop();
            });

            log.info("netty transport server started");
        } catch (Exception e) {
            log.error("start netty transport server error", e);
        }
    }

    private void stop() {
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

    private static class Initializer extends ChannelInitializer {
        @Override
        protected void initChannel(Channel channel) throws Exception {
            channel.pipeline()
                    .addLast();
        }
    }
}
