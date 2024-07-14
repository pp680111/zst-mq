package com.zst.mq.broker.core;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

/**
 * 消息实体
 */
@Getter
@Setter
public class Message {
    private Map<String, String> properties = new HashMap<>();
    private String content;

    public String getQueueName() {
        return properties.get("queue");
    }

    public String getMessageId() {
        return properties.get("messageId");
    }

    public long getOffset() {
        if (properties.get("offset") == null) {
            return -1;
        }
        return Long.parseLong(properties.get("offset"));
    }

    public void setQueueName(String queueName) {
        this.properties.put("queue", queueName);
    }

    public void setMessageId(String messageId) {
        this.properties.put("messageId", messageId);
    }

}
