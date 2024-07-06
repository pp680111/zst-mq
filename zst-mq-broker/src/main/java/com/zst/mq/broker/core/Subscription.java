package com.zst.mq.broker.core;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Subscription {
    private String consumerId;
    private String queueName;
    private long offset;
}
