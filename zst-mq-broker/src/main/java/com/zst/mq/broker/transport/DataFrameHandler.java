package com.zst.mq.broker.transport;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.broker.core.ActionFrame;
import com.zst.mq.broker.core.ActionHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataFrameHandler extends ChannelInboundHandlerAdapter {
    private ActionHandler actionHandler;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        TransportFrame frame = (TransportFrame) msg;
        try {
            ActionFrame actionFrame = JSON.parseObject(frame.actionFrameContent, ActionFrame.class);
            ActionFrame response = actionHandler.dispatch(actionFrame);
            TransportFrame responseTransportFrame = new TransportFrame();
            responseTransportFrame.setActionFrameContent(JSON.toJSONString(response));
            ctx.writeAndFlush(responseTransportFrame);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
