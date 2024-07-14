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

import java.io.Closeable;
import java.io.IOException;

@Slf4j
public class NettyTransport implements Closeable {
    private EventLoopGroup eventLoopGroup;
    private Channel channel;
    private BrokerProperties brokerProperties;
    private ResponseFutureHolder responseFutureHolder;

    public NettyTransport(BrokerProperties brokerProperties) {
        this.brokerProperties = brokerProperties;
        responseFutureHolder = new ResponseFutureHolder();
    }

    @Override
    public void close() {
        stop();
    }

    public void start() {
        eventLoopGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());

        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(eventLoopGroup)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<NioSocketChannel>() {
                        @Override
                        protected void initChannel(NioSocketChannel channel) throws Exception {
                            channel.pipeline()
                                    .addLast(new FrameDecoder())
                                    .addLast(new FrameEncoder())
                                    .addLast(new BrokerResponseHandler(responseFutureHolder));
                        }
                    });

            channel = bootstrap
                    .connect(brokerProperties.getHost(), brokerProperties.getPort())
                    .sync()
                    .channel();

            channel.closeFuture().addListener(future -> {
                stop();
            });

            log.info("netty client connection start");
        } catch (Exception e) {
            log.info("netty client connect failed", e);
        }
    }

    public ResponseFuture send(TransportFrame frame, boolean sync, boolean requireResponseFuture) {
        if (channel == null) {
            throw new RuntimeException("client not init yet");
        }

        ResponseFuture responseFuture = null;
        try {
            if (requireResponseFuture) {
                responseFuture = responseFutureHolder.register(frame);
            }

            ChannelFuture future = channel.writeAndFlush(frame);
            if (sync) {
                future.syncUninterruptibly();
            }
        } catch (Exception e) {
            if (responseFuture != null) {
                responseFutureHolder.cancelFuture(frame.getSeqNo());
            }
            log.error(e.getMessage(), e);
        }

        return responseFuture;
    }

    public void stop() {
        if (channel != null) {
            try {
                channel.close().sync();
            } catch (Exception e) {
                log.error("关闭客户端连接时发生错误", e);
            }
        }

        if (eventLoopGroup != null && !eventLoopGroup.isShutdown()) {
            eventLoopGroup.shutdownGracefully();
            eventLoopGroup = null;
        }
    }


}
