package com.zst.mq.broker.core.frame;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.broker.core.ActionFrame;
import com.zst.mq.broker.core.Message;
import lombok.Getter;

@Getter
public class PublishMessageFrame {
    private Message message;

    public static PublishMessageFrame fromActionFrame(ActionFrame frame) {
        if (frame == null) {
            throw new IllegalArgumentException();
        }

        try {
            return JSON.parseObject(frame.getContent(), PublishMessageFrame.class);
        } catch (Exception e) {
            throw new IllegalArgumentException();
        }
    }
}
