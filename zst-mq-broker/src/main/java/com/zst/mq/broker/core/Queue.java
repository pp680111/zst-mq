package com.zst.mq.broker.core;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Queue {
    private Deque<QueueMessage> queue;
    private String name;

    public Queue(String name) {
        this.name = name;
        queue = new LinkedList<>();
    }

    /**
     * 获取队列的当前偏移量
     * @return
     */
    public long currentOffset() {
        QueueMessage headMessage = queue.peekLast();
        if (headMessage != null) {
            return headMessage.getOffset();
        }

        return 0;
    }

    /**
     * 添加消息
     * @param message
     * @return
     */
    public long addMessage(Message message) {
        synchronized (this) {
            QueueMessage queueMessage = new QueueMessage();
            queueMessage.setMessage(message);
            queueMessage.setOffset(currentOffset() + 1);
            queue.add(queueMessage);
            return queueMessage.getOffset();
        }
    }

    /**
     * 从指定的offset开始，获取指定最大数量的消息
     * @param beginOffset
     * @param batchNum
     * @return
     */
    public List<Message> fetchMessage(long beginOffset, int batchNum) {
        if (queue.isEmpty() || queue.peekLast().getOffset() <= beginOffset) {
            return Collections.emptyList();
        }

        List<Message> result = new ArrayList<>();
        for (QueueMessage queueMessage : queue) {
            if (queueMessage.getOffset() > beginOffset) {
                result.add(queueMessage.getMessageForFetch());
                if (result.size() >= batchNum) {
                    break;
                }
            }
        }

        return result;
    }

    /**
     * 在Queue中使用的消息结构，对消息体进行了额外的封装
     */
    @Setter
    @Getter
    private class QueueMessage {
        private Message message;
        private long offset;

        /**
         * 将包裹的消息复制一份返回
         *
         * 复制的目的是防止消息体的引用被拿出去之后修改了，保持消息本身的不变
         * @return
         */
        public Message getMessageForFetch() {
            Message message = new Message();
            message.setContent(this.message.getContent());

            Map<String, String> originProperties = this.message.getProperties();
            Map<String, String> newProperties = new HashMap<>();
            newProperties.put("offset", Long.toString(this.offset));
            newProperties.putAll(originProperties);
            message.setProperties(newProperties);
            return message;
        }
    }
}
