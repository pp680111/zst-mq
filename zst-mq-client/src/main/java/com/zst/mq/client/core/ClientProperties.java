package com.zst.mq.client.core;


import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ClientProperties {
    /**
     * 等待Broker响应的超时时间
     */
    private long responseTimeoutMs = 10 * 1000L;
}
