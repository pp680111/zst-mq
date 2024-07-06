package com.zst.mq.broker.core;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Queue {
    private BlockingQueue<QueueMessage> queue;
    private String name;

    public Queue() {
        queue = new LinkedBlockingQueue<>();
    }

    /**
     * 获取队列的当前偏移量
     * @return
     */
    public long currentOffset() {
        QueueMessage headMessage = queue.peek();
        if (headMessage != null) {
            return headMessage.getOffset();
        }

        return 0;
    }

    /**
     * 在Queue中使用的消息结构，对消息体进行了额外的封装
     */
    @Setter
    @Getter
    private class QueueMessage {
        private Message message;
        private long offset;
    }
}
