package com.zst.mq.client.core;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.broker.core.ActionFrame;
import com.zst.mq.broker.core.ActionType;
import com.zst.mq.broker.core.frame.FetchOffsetResponseFrame;
import com.zst.mq.broker.core.frame.SubscribeRequestFrame;
import com.zst.mq.broker.transport.TransportFrame;
import com.zst.mq.broker.utils.StringUtils;
import com.zst.mq.client.transport.NettyTransport;
import com.zst.mq.client.transport.ResponseFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MQClient {
    private ClientProperties clientProperties;
    private NettyTransport transport;
    private ScheduledExecutorService executor;

    public MQClient() {
    }

    public MQClient(ClientProperties clientProperties, NettyTransport transport) {
        this.transport = transport;
        this.clientProperties = clientProperties;
        executor = Executors.newSingleThreadScheduledExecutor();
        init();
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

        try {
            ResponseFuture future = transport.send(transportFrame, true, true);
            TransportFrame response = future.get(clientProperties.getResponseTimeoutMs(), TimeUnit.MILLISECONDS);
            ActionFrame responseActionFrame = unwrapTransportFrame(response);

            if (responseActionFrame.getAction() != ActionType.OK) {
                // TODO 异常处理待完善
                throw new RuntimeException("请求失败");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 查询消费者的消费进度
     * @param consumerId
     * @return
     */
    public Map<String, Long> fetchOffset() {
        ActionFrame actionFrame = createActionFrame(ActionType.FETCH_CONSUMPTION_OFFSET, clientProperties.getConsumerId(),
                null, null);
        TransportFrame transportFrame = wrapTransportFrame(actionFrame);

        try {
            ResponseFuture future = transport.send(transportFrame, true, true);
            TransportFrame response = future.get(clientProperties.getResponseTimeoutMs(), TimeUnit.MILLISECONDS);
            ActionFrame responseActionFrame = unwrapTransportFrame(response);

            if (responseActionFrame.getAction() != ActionType.FETCH_CONSUMPTION_OFFSET_RESPONSE) {
                // TODO 异常处理待完善
                throw new RuntimeException("请求失败");
            }

            FetchOffsetResponseFrame fetchOffsetResponseFrame = JSON.parseObject(responseActionFrame.getContent(),
                    FetchOffsetResponseFrame.class);
            return fetchOffsetResponseFrame.getCurrentSubscriptionOffset();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void init() {
        scheduleHeartbeat();
    }

    private void scheduleHeartbeat() {
        executor.scheduleAtFixedRate(() -> {
            sendHeartbeat();
            log.debug("send heartbeat");
        }, 0, clientProperties.getHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeat() {
        ActionFrame frame = new ActionFrame(ActionType.HEARTBEAT);
        frame.setConsumerId(clientProperties.getConsumerId());

        TransportFrame transportFrame = wrapTransportFrame(frame);
        transport.send(transportFrame, true, false);
    }

    private ActionFrame createActionFrame(int action, String consumerId, Object content,
                                          Map<String, String> properties) {
        ActionFrame actionFrame = new ActionFrame(action);
        actionFrame.setConsumerId(consumerId);
        if (content != null) {
            actionFrame.setContent(JSON.toJSONString(content));
        }
        actionFrame.addProperties(properties);
        return actionFrame;
    }

    private TransportFrame wrapTransportFrame(ActionFrame frame) {
        TransportFrame transportFrame = new TransportFrame();
        transportFrame.setActionFrameContent(JSON.toJSONString(frame));
        return transportFrame;
    }

    private ActionFrame unwrapTransportFrame(TransportFrame transportFrame) {
        return JSON.parseObject(transportFrame.getActionFrameContent(), ActionFrame.class);
    }
}
