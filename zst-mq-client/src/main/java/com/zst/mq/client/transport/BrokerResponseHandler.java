package com.zst.mq.client.transport;

import com.zst.mq.broker.transport.TransportFrame;
import com.zst.mq.client.core.MQClient;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.AllArgsConstructor;

/**
 * 处理Broker响应的Handler
 */
@AllArgsConstructor
public class BrokerResponseHandler extends ChannelInboundHandlerAdapter {
    private MQClient mqClient;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        TransportFrame transportFrame = (TransportFrame) msg;
        mqClient.handleTransportResponse(transportFrame);
    }
}
