package com.zst.mq.client.core;

import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
public class ConsumerProperties {
    /**
     * 心跳包间隔
     */
    private long heartbeatIntervalMs = 2 * 1000;
    /**
     * 消费者ID
     */
    private String consumerId = UUID.randomUUID().toString().replaceAll("-", "");
    /**
     * 拉取消息最大数量
     */
    private int maxBatch = 10;
}
