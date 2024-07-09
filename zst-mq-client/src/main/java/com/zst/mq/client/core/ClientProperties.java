package com.zst.mq.client.core;


import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
public class ClientProperties {
    /**
     * 心跳包间隔
     */
    private long heartbeatIntervalMs = 1000;
    /**
     * 消费者ID
     */
    private String consumerId = UUID.randomUUID().toString().replaceAll("-", "");
}
