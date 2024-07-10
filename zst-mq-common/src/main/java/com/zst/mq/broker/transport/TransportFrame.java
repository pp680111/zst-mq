package com.zst.mq.broker.transport;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TransportFrame {
    /**
     * 传输帧的序列号
     */
    long seqNo;
    /**
     * 传输帧的内容
     */
    String actionFrameContent;
}
