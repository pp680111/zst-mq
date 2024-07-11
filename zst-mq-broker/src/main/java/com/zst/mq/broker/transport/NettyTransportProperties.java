package com.zst.mq.broker.transport;

import lombok.Getter;
import lombok.Setter;

/**
 * netty传输层参数
 */
@Getter
@Setter
public class NettyTransportProperties {
    /**
     * 服务端口
     */
    private int port = 6464;
    /**
     * io线程数量
     */
    private int ioWorkerNum = 1;
    /**
     * 业务线程数量
     */
    private int bizWorkerNum = Runtime.getRuntime().availableProcessors();
}
