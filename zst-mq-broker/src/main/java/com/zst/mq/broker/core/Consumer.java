package com.zst.mq.broker.core;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Consumer {
    private String consumerId;
    private long lastHeartbeatTime;
}
