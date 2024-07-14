package com.zst.mq.client.core;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.broker.core.ActionFrame;
import com.zst.mq.broker.core.ActionType;
import com.zst.mq.broker.core.Message;
import com.zst.mq.broker.core.frame.FetchMessageRequestFrame;
import com.zst.mq.broker.core.frame.FetchMessageResponseFrame;
import com.zst.mq.broker.core.frame.FetchOffsetResponseFrame;
import com.zst.mq.broker.core.frame.OffsetCommitRequestFrame;
import com.zst.mq.broker.core.frame.PublishMessageFrame;
import com.zst.mq.broker.core.frame.SubscribeRequestFrame;
import com.zst.mq.broker.transport.TransportFrame;
import com.zst.mq.broker.utils.StringUtils;
import com.zst.mq.client.transport.NettyTransport;
import com.zst.mq.client.transport.ResponseFuture;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MQClient implements Closeable {
    private ClientProperties clientProperties;
    private NettyTransport transport;

    public MQClient() {
    }

    public MQClient(ClientProperties clientProperties, NettyTransport transport) {
        this.transport = transport;
        this.clientProperties = clientProperties;
    }

    @Override
    public void close() {
        transport.close();
    }

    /**
     * 订阅队列
     * @param queueName
     */
    public void subscribeQueue(String consumerId, String queueName) {
        if (StringUtils.isEmpty(queueName)) {
            throw new IllegalArgumentException();
        }

        ActionFrame frame = new ActionFrame(ActionType.SUBSCRIBE_QUEUE);
        frame.setConsumerId(consumerId);

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
    public Map<String, Long> fetchOffset(String consumerId) {
        if (StringUtils.isEmpty(consumerId)) {
            throw new IllegalArgumentException();
        }

        ActionFrame actionFrame = createActionFrame(ActionType.FETCH_CONSUMPTION_OFFSET, consumerId,
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

    /**
     * 发布消息
     * @param message
     * @param requireAck
     */
    public void publishMessage(Message message, boolean requireAck) {
        PublishMessageFrame pmf = new PublishMessageFrame();
        pmf.setMessage(message);

        ActionFrame actionFrame = createActionFrame(ActionType.PUBLISH_MESSAGE, null,
                pmf, null);
        TransportFrame transportFrame = wrapTransportFrame(actionFrame);

        try {
            ResponseFuture future = transport.send(transportFrame, true, requireAck);

            // 如果不需要回复的话，则确认等待消息已经发送出去之后就可以返回了
            if (!requireAck) {
                return;
            }

            TransportFrame responseFrame = future.get(clientProperties.getResponseTimeoutMs(), TimeUnit.MILLISECONDS);
            ActionFrame responseActionFrame = unwrapTransportFrame(responseFrame);

            if (responseActionFrame.getAction() != ActionType.PUBLISH_ACK) {
                throw new RuntimeException();
            }
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    /**
     * 获取消息
     * @param consumerId
     * @param queueName
     * @param beginOffset
     * @param maxBatch
     * @return
     */
    public List<Message> fetchMessage(String consumerId, String queueName, long beginOffset, int maxBatch) {
        FetchMessageRequestFrame requestFrame = new FetchMessageRequestFrame();
        requestFrame.setQueueName(queueName);
        requestFrame.setMaxBatch(maxBatch);
        requestFrame.setBeginOffset(beginOffset);

        ActionFrame actionFrame = createActionFrame(ActionType.FETCH_MESSAGE, consumerId,
                requestFrame, null);
        TransportFrame transportFrame = wrapTransportFrame(actionFrame);

        try {
            ResponseFuture future = transport.send(transportFrame, true, true);
            TransportFrame response = future.get(clientProperties.getResponseTimeoutMs(), TimeUnit.MILLISECONDS);
            ActionFrame responseActionFrame = unwrapTransportFrame(response);

            if (responseActionFrame.getAction() != ActionType.FETCH_MESSAGE_RESPONSE) {
                // TODO 异常处理待完善
                throw new RuntimeException("请求失败");
            }

            FetchMessageResponseFrame fetchMessageResponseFrame = JSON.parseObject(responseActionFrame.getContent(),
                    FetchMessageResponseFrame.class);
            return fetchMessageResponseFrame.getMessages();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 提交指定consumerId对指定队列的消费偏移量
     * @param consumerId
     * @param queueName
     * @param offset
     */
    public void commitOffset(String consumerId, String queueName, long offset) {
        OffsetCommitRequestFrame requestFrame = new OffsetCommitRequestFrame();
        requestFrame.setQueueName(queueName);
        requestFrame.setOffset(offset);

        ActionFrame actionFrame = createActionFrame(ActionType.SUBMIT_OFFSET, consumerId,
                requestFrame, null);
        TransportFrame transportFrame = wrapTransportFrame(actionFrame);

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

    public void sendHeartbeat(String consumerId) {
        ActionFrame frame = new ActionFrame(ActionType.HEARTBEAT);
        frame.setConsumerId(consumerId);

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
