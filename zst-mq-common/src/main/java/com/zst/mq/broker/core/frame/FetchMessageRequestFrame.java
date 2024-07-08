package com.zst.mq.broker.core.frame;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.broker.core.ActionFrame;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 *
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class FetchMessageRequestFrame {
    private String queueName;
    private long beginOffset;
    private int maxBatch;

    public static FetchMessageRequestFrame fromActionFrame(ActionFrame frame) {
        if (frame == null) {
            throw new IllegalArgumentException();
        }

        try {
            return JSON.parseObject(frame.getContent(), FetchMessageRequestFrame.class);
        } catch (Exception e) {
            throw new IllegalArgumentException();
        }
    }
}
