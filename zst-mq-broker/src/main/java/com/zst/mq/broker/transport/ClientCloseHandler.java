package com.zst.mq.broker.transport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.text.MessageFormat;

@Slf4j
public class ClientCloseHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      log.info(MessageFormat.format("远程地址为{0}的客户端已断开连接", ctx.channel().remoteAddress()));
    }

}
