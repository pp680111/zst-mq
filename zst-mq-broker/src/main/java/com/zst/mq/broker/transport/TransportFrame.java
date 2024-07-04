package com.zst.mq.broker.transport;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TransportFrame {
    /**
     * 命令类型
     */
    private int command;
    /**
     * 内容
     */
    private byte[] body;
}
