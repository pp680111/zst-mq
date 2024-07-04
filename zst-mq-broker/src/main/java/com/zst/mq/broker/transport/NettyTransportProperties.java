package com.zst.mq.broker.transport;

import lombok.Getter;
import lombok.Setter;

/**
 * netty传输层参数
 */
@Getter
@Setter
public class NettyTransportProperties {
    private int port = 6464;
    private int ioWorker = 1;
    private int bizWorker = Runtime.getRuntime().availableProcessors();
}
