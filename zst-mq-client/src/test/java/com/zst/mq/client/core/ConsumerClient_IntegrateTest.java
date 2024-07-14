package com.zst.mq.client.core;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.client.transport.BrokerProperties;
import com.zst.mq.client.transport.NettyTransport;
import com.zst.mq.client.utils.PrivateAccessor;
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
        consumerProperties.setConsumerId("zst");

        ConsumerClient cc = new ConsumerClient(client, "zst-queue", consumerProperties);
        cc.start();
    }

    @Test
    public void testFetchMessage() {
        BrokerProperties bp = new BrokerProperties();
        bp.setHost("127.0.0.1");
        bp.setPort(6464);

        NettyTransport nettyTransport = new NettyTransport(bp);
        nettyTransport.start();

        ClientProperties cp = new ClientProperties();
        MQClient client = new MQClient(cp, nettyTransport);

        ConsumerProperties consumerProperties = new ConsumerProperties();
        consumerProperties.setConsumerId("zst");

        ConsumerClient cc = new ConsumerClient(client, "zst-queue", consumerProperties);
        cc.start();

        System.err.println(JSON.toJSONString(cc.fetchMessage()));
        System.err.println((long) PrivateAccessor.get(cc, "currentOffset"));
    }

    @Test
    public void testCommitConsumedOffset() {
        BrokerProperties bp = new BrokerProperties();
        bp.setHost("127.0.0.1");
        bp.setPort(6464);

        NettyTransport nettyTransport = new NettyTransport(bp);
        nettyTransport.start();

        ClientProperties cp = new ClientProperties();
        MQClient client = new MQClient(cp, nettyTransport);

        ConsumerProperties consumerProperties = new ConsumerProperties();
        consumerProperties.setConsumerId("zst");

        ConsumerClient cc = new ConsumerClient(client, "zst-queue", consumerProperties);
        cc.start();

        System.err.println(JSON.toJSONString(cc.fetchMessage()));
        System.err.println((long) PrivateAccessor.get(cc, "currentOffset"));

        cc.commitOffset();

        System.err.println(JSON.toJSONString(cc.fetchMessage()));
        System.err.println((long) PrivateAccessor.get(cc, "currentOffset"));
    }
}
