package com.zst.mq.broker.transport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.net.SocketException;
import java.text.MessageFormat;

@Slf4j
public class ClientExceptionHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof SocketException) {
            log.error(MessageFormat.format("远程连接{0}出现异常: {1}", ctx.channel().remoteAddress(), cause.getMessage()));
        }
    }
}
