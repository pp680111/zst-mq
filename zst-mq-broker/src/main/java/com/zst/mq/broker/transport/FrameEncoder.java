package com.zst.mq.broker.transport;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.broker.utils.HexUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

/**
 * 消息体的编码器
 *
 */
public class FrameEncoder extends MessageToByteEncoder<TransportFrame> {
    @Override
    protected void encode(ChannelHandlerContext ctx, TransportFrame msg, ByteBuf out) throws Exception {
        if (msg == null) {
            return;
        }

        String jsonContent = JSON.toJSONString(msg);
        String hexEncodedContent = HexUtils.toHexString(jsonContent);
        out.writeBytes(hexEncodedContent.getBytes(StandardCharsets.UTF_8));
    }
}
