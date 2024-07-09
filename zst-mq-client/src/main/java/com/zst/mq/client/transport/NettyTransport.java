package com.zst.mq.client.transport;

import com.zst.mq.broker.transport.FrameDecoder;
import com.zst.mq.broker.transport.FrameEncoder;
import com.zst.mq.broker.transport.TransportFrame;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettyTransport {
    private EventLoopGroup eventLoopGroup;
    private Channel channel;
    private BrokerProperties brokerProperties;

    public void start() {
        eventLoopGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());

        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(eventLoopGroup)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .channel(NioSocketChannel.class)
                    .handler(new Initializer());

            channel = bootstrap.connect(brokerProperties.getHost(),
                    brokerProperties.getPort()).channel();

            channel.closeFuture().addListener(future -> {
                stop();
            });

            log.info("netty client connection start");
        } catch (Exception e) {
            log.info("netty client connect failed", e);
        }
    }

    public void stop() {
        if (eventLoopGroup != null && !eventLoopGroup.isShutdown()) {
            eventLoopGroup.shutdownGracefully();
            eventLoopGroup = null;
        }
    }

    public void send(TransportFrame frame, boolean sync) {
        if (channel == null) {
            throw new RuntimeException("client not init yet");
        }

        ChannelFuture future = channel.write(frame);
        if (sync) {
            future.syncUninterruptibly();
        }
    }

    private static class Initializer extends ChannelInitializer {
        @Override
        protected void initChannel(Channel channel) throws Exception {
            channel.pipeline()
                    .addLast(new FrameDecoder())
                    .addLast(new FrameEncoder());
        }
    }
}
