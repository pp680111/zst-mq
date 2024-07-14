package com.zst.mq.client.core;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.client.transport.BrokerProperties;
import com.zst.mq.client.transport.NettyTransport;
import com.zst.mq.client.utils.PrivateAccessor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;

public class MQClient_IntegrateTest {
    @Test
    public void testSendHeartbeat() throws IOException {
        BrokerProperties bp = new BrokerProperties();
        bp.setHost("127.0.0.1");
        bp.setPort(6464);
        NettyTransport nettyTransport = new NettyTransport(bp);
        nettyTransport.start();

        MQClient client = new MQClient();
        ClientProperties cp = new ClientProperties();
        PrivateAccessor.set(client, "clientProperties", cp);
        PrivateAccessor.set(client, "transport", nettyTransport);

        String consumerId = UUID.randomUUID().toString();
        client.sendHeartbeat(consumerId);

        client.close();
    }

//    @Test
//    public void testHangOnForDuration() {
//        BrokerProperties bp = new BrokerProperties();
//        bp.setHost("127.0.0.1");
//        bp.setPort(6464);
//
//        NettyTransport nettyTransport = new NettyTransport(bp);
//        nettyTransport.start();
//
//        ClientProperties cp = new ClientProperties();
//        MQClient client = new MQClient(cp, nettyTransport);
//
//        LockSupport.parkNanos(Duration.ofMinutes(1).toNanos());
//    }

    @Test
    public void testSubscribeQueue() {
        BrokerProperties bp = new BrokerProperties();
        bp.setHost("127.0.0.1");
        bp.setPort(6464);

        NettyTransport nettyTransport = new NettyTransport(bp);
        nettyTransport.start();

        ClientProperties cp = new ClientProperties();
        MQClient client = new MQClient(cp, nettyTransport);

        String consumerId = UUID.randomUUID().toString();

        client.sendHeartbeat(consumerId);
        client.subscribeQueue(consumerId, "zst-queue");
    }

    @Test
    public void testFetchOffset() {
        BrokerProperties bp = new BrokerProperties();
        bp.setHost("127.0.0.1");
        bp.setPort(6464);

        NettyTransport nettyTransport = new NettyTransport(bp);
        nettyTransport.start();

        ClientProperties cp = new ClientProperties();
        MQClient client = new MQClient(cp, nettyTransport);

        String consumerId = UUID.randomUUID().toString();
        client.sendHeartbeat(consumerId);
        client.subscribeQueue(consumerId, "zst-queue");

        Map<String, Long> offset = client.fetchOffset(consumerId);
        Assertions.assertTrue(offset.containsKey("zst-queue"));
        System.err.println(JSON.toJSONString(offset));
    }
}
