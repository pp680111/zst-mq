package com.zst.mq.broker.transport;

import com.zst.mq.broker.core.ActionHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class DataFrameHandler extends ChannelInboundHandlerAdapter {
    private ActionHandler actionHandler;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        TransportFrame frame = (TransportFrame) msg;
    }
}
