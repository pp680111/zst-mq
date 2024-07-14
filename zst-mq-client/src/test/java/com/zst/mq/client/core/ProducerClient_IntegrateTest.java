package com.zst.mq.client.core;

import com.zst.mq.client.transport.BrokerProperties;
import com.zst.mq.client.transport.NettyTransport;
import org.junit.jupiter.api.Test;

public class ProducerClient_IntegrateTest {
    @Test
    public void testPublishMessage() {
        BrokerProperties bp = new BrokerProperties();
        bp.setHost("127.0.0.1");
        bp.setPort(6464);

        NettyTransport nettyTransport = new NettyTransport(bp);
        nettyTransport.start();

        ClientProperties cp = new ClientProperties();
        MQClient client = new MQClient(cp, nettyTransport);

        ProducerClient pc = new ProducerClient(client);
        pc.publish("zst-queue", "hello world", true);
    }
}
