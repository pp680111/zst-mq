package com.zst.mq.client.core;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.broker.core.ActionFrame;
import com.zst.mq.broker.core.ActionType;
import com.zst.mq.broker.core.frame.SubscribeRequestFrame;
import com.zst.mq.broker.transport.TransportFrame;
import com.zst.mq.broker.utils.StringUtils;
import com.zst.mq.client.transport.NettyTransport;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MQClient {
    private ClientProperties clientProperties;
    private NettyTransport transport;
    private ScheduledExecutorService executor;
    private ConcurrentHashMap<Long, CompletableFuture<?>> transportFutureMap;

    public MQClient(ClientProperties clientProperties, NettyTransport transport) {
        this.transport = transport;
        this.clientProperties = clientProperties;
        this.transportFutureMap = new ConcurrentHashMap<>(1024);
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * 订阅队列
     * @param queueName
     */
    public void subscribeQueue(String queueName) {
        if (StringUtils.isEmpty(queueName)) {
            throw new IllegalArgumentException();
        }

        ActionFrame frame = new ActionFrame(ActionType.SUBSCRIBE_QUEUE);
        frame.setConsumerId(clientProperties.getConsumerId());

        SubscribeRequestFrame subscribeRequestFrame = new SubscribeRequestFrame();
        subscribeRequestFrame.setQueueName(queueName);
        frame.setContent(JSON.toJSONString(subscribeRequestFrame));

        TransportFrame transportFrame = wrapTransportFrame(frame);
        transport.send(transportFrame, true);
    }

    public void handleTransportResponse(TransportFrame transportFrame) {

    }

    private void init() {
        scheduleHeartbeat();
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
        return transportFrame;
    }
}
