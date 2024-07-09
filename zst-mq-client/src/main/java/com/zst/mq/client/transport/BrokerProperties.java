package com.zst.mq.client.transport;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BrokerProperties {
    private String host;
    private int port;
}
