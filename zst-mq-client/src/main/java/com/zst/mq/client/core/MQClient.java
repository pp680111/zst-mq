package com.zst.mq.client.core;

import com.zst.mq.broker.core.ActionFrame;
import com.zst.mq.broker.core.ActionType;
import com.zst.mq.broker.transport.TransportFrame;
import com.zst.mq.client.transport.NettyTransport;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MQClient {
    private ClientProperties clientProperties;
    private NettyTransport transport;
    private ScheduledExecutorService executor;

    private void init() {

    }

    private void scheduleHeartbeat() {
        executor.scheduleAtFixedRate(() -> {
            sendHeartbeat();
            log.debug("send heartbeat");
        }, 1000, clientProperties.getHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeat() {
        ActionFrame frame = new ActionFrame(ActionType.HEARTBEAT);
        frame.setConsumerId(clientProperties.getConsumerId());

        TransportFrame transportFrame = wrapTransportFrame(frame);
        transport.send(transportFrame, false);
    }

    private TransportFrame wrapTransportFrame(ActionFrame frame) {
        TransportFrame transportFrame = new TransportFrame();
        transportFrame.setActionFrameContent(frame.getContent());
        return transportFrame;;
    }
}
