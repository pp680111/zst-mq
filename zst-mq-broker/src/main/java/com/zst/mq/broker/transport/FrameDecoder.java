package com.zst.mq.broker.transport;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.broker.utils.HexUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * 消息体的解码器
 *
 * 消息体在传输时以JSON字符串的二进制数据的方式传输
 */
@Slf4j
public class FrameDecoder extends ByteToMessageDecoder {
    /**
     * 默认最大单个Frame的大小
     */
    private static final int MAX_LENGTH = 4 * 1024 * 1024;

    private StringBuilder buffer;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (buffer == null) {
            buffer = new StringBuilder();
        }

        int readableBytes = in.readableBytes();
        for (int i = 0; i < readableBytes; i++) {
            buffer.append((char) in.readByte());

            if (buffer.length() > MAX_LENGTH) {
                log.error("Frame too large, drop frame", MAX_LENGTH);
            }

            if (buffer.length() > 2 && buffer.charAt(buffer.length() - 2) == '\r' && buffer.charAt(buffer.length() - 1) == '\n') {
                String result = finishBuffer();
                TransportFrame frame = decodeFrame(result);
                if (frame != null) {
                    out.add(frame);
                }
            }
        }
    }

    private String finishBuffer() {
        String frameRawData = buffer.substring(0, buffer.length() - 2);
        buffer = null;
        return HexUtils.hex2String(frameRawData);
    }
    
    private TransportFrame decodeFrame(String frameRawData) {
        try {
            return JSON.parseObject(frameRawData, TransportFrame.class);
        } catch (Exception e) {
            log.error("frame decode error, drop it, raw content = {}", frameRawData);
        }

        return null;
    }
}
