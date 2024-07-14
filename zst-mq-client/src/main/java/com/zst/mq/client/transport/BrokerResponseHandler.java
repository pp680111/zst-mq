package com.zst.mq.client.transport;

import com.zst.mq.broker.transport.TransportFrame;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 处理Broker响应的Handler
 */
@Slf4j
@AllArgsConstructor
public class BrokerResponseHandler extends ChannelInboundHandlerAdapter {
    private ResponseFutureHolder responseFutureHolder;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        TransportFrame transportFrame = (TransportFrame) msg;
        try {
            responseFutureHolder.completeFuture(transportFrame);
        } catch (Exception e) {
            log.error("处理Broker端响应时发生错误");
        }
    }
}
