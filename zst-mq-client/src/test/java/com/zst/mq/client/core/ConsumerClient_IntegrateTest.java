package com.zst.mq.client.core;

import com.zst.mq.client.transport.BrokerProperties;
import com.zst.mq.client.transport.NettyTransport;
import org.junit.jupiter.api.Test;

public class ConsumerClient_IntegrateTest {
    @Test
    public void testStart() {
        BrokerProperties bp = new BrokerProperties();
        bp.setHost("127.0.0.1");
        bp.setPort(6464);

        NettyTransport nettyTransport = new NettyTransport(bp);
        nettyTransport.start();

        ClientProperties cp = new ClientProperties();
        MQClient client = new MQClient(cp, nettyTransport);

        ConsumerProperties consumerProperties = new ConsumerProperties();

        ConsumerClient cc = new ConsumerClient(client, "zst-queue", consumerProperties);
        cc.start();
    }
}
