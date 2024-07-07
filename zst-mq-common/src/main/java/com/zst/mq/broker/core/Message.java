package com.zst.mq.broker.core;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * 消息实体
 */
@Getter
@Setter
public class Message {
    private Map<String, String> properties;
    private String content;

    public String getQueueName() {
        return properties.get("queue");
    }

    public String getMessageId() {
        return properties.get("messageId");
    }
}
