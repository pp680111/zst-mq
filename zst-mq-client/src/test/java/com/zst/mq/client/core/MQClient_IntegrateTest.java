package com.zst.mq.client.core;

import com.zst.mq.client.transport.BrokerProperties;
import com.zst.mq.client.transport.NettyTransport;
import com.zst.mq.client.utils.PrivateAccessor;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

public class MQClient_IntegrateTest {
    @Test
    public void testSendHeartbeat() {
        BrokerProperties bp = new BrokerProperties();
        bp.setHost("127.0.0.1");
        bp.setPort(6464);
        NettyTransport nettyTransport = new NettyTransport(bp);
        nettyTransport.start();

        MQClient client = new MQClient();
        ClientProperties cp = new ClientProperties();
        PrivateAccessor.set(client, "clientProperties", cp);
        PrivateAccessor.set(client, "transport", nettyTransport);

        PrivateAccessor.invoke(client, "sendHeartbeat");

        LockSupport.parkNanos(Duration.ofSeconds(3).toNanos());
    }
}
