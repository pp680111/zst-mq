package com.zst.mq.broker.core.frame;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.broker.core.ActionFrame;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OffsetCommitRequestFrame {
    private String queueName;
    private Long offset;

    public static OffsetCommitRequestFrame fromActionFrame(ActionFrame frame) {
        if (frame == null) {
            throw new IllegalArgumentException("");
        }

        try {
            return JSON.parseObject(frame.getContent(), OffsetCommitRequestFrame.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("");
        }
    }
}
