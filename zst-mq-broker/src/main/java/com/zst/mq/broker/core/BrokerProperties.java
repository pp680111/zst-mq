package com.zst.mq.broker.core;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BrokerProperties {
    /**
     * 存储路径
     */
    private String storagePath = ".";
}
