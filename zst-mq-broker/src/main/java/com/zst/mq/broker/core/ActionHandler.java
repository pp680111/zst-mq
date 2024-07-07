package com.zst.mq.broker.core;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.broker.core.exception.BrokerException;
import com.zst.mq.broker.core.frame.*;
import com.zst.mq.broker.utils.StringUtils;

import java.util.List;
import java.util.Map;

public class ActionHandler {
    private Broker broker;

    public ActionHandler(Broker broker) {
        if (broker == null) {
            throw new IllegalArgumentException("broker is null");
        }
        this.broker = broker;
    }

    public ActionFrame dispatch(ActionFrame frame) {
        switch (frame.getAction()) {
            case ActionType.HEARTBEAT:
                return handleHeartbeat(frame);
            case ActionType.SUBSCRIBE_QUEUE:
                handleSubscribeQueue(frame);
                break;
            case ActionType.UNSUBSCRIBE_QUEUE:
                break;
            case ActionType.PUBLISH_MESSAGE:
                return handlePublishMessage(frame);
            case ActionType.FETCH_CONSUMPTION_OFFSET:
                return handleFetchConsumptionOffset(frame);
            case ActionType.FETCH_MESSAGE:
                return handleFetchMessage(frame);
            case ActionType.SUBMIT_OFFSET:
                break;
        }

        return null;
    }

    /**
     * 处理心跳请求
     * @param frame
     * @return
     */
    private ActionFrame handleHeartbeat(ActionFrame frame) {
        String consumerId = frame.getConsumerId();
        if (StringUtils.isNotEmpty(consumerId)) {
            broker.updateConsumerHeartbeat(consumerId);
            return CommonReply.OK;
        }

        return CommonReply.REQUEST_ERROR;
    }

    /**
     * 订阅队列
     * @param frame
     * @return
     */
    private ActionFrame handleSubscribeQueue(ActionFrame frame) {
        String consumerId = frame.getConsumerId();
        SubscribeRequestFrame request = SubscribeRequestFrame.fromActionFrame(frame);

        broker.addSubscription(consumerId, request.getQueueName());
        return CommonReply.OK;
    }

    /**
     * 处理消费offset进度查询请求
     * @param frame
     * @return
     */
    private ActionFrame handleFetchConsumptionOffset(ActionFrame frame) {
        String consumerId = frame.getConsumerId();
        Map<String, Long> queueOffsetMap = broker.queryConsumerSubscriptionOffsets(consumerId);

        FetchOffsetResponseFrame response = new FetchOffsetResponseFrame(queueOffsetMap);

        ActionFrame result = new ActionFrame();
        result.setAction(ActionType.FETCH_CONSUMPTION_OFFSET_RESPONSE);
        result.setContent(JSON.toJSONString(response));
        return result;
    }

    /**
     * 发布消息
     * @param frame
     * @return
     */
    private ActionFrame handlePublishMessage(ActionFrame frame) {
        PublishMessageFrame publishMessageFrame = PublishMessageFrame.fromActionFrame(frame);

        if (publishMessageFrame.getMessage() == null) {
            return CommonReply.REQUEST_ERROR;
        }

        PublishAckFrame publishAckFrame = new PublishAckFrame();
        publishAckFrame.setMessageId(publishMessageFrame.getMessage().getMessageId());

        try {
            broker.publishMessages(publishMessageFrame.getMessage());
            publishAckFrame.setSuccess(true);
        } catch (BrokerException e) {
            publishAckFrame.setCause(e.getMessage());
            publishAckFrame.setSuccess(false);
        } catch (Exception e) {
            publishAckFrame.setCause("未知错误");
            publishAckFrame.setSuccess(false);
        }

        ActionFrame result = new ActionFrame();
        result.setAction(ActionType.PUBLISH_ACK);
        result.setContent(JSON.toJSONString(publishAckFrame));
        return result;
    }

    /**
     * 处理拉取消息的请求
     * @param frame
     * @return
     */
    private ActionFrame handleFetchMessage(ActionFrame frame) {
        FetchMessageRequestFrame requestFrame = FetchMessageRequestFrame.fromActionFrame(frame);
        if (StringUtils.isEmpty(requestFrame.getQueueName())) {
            return CommonReply.REQUEST_ERROR;
        }

        List<Message> fetchResult = broker.fetchMessage(frame.getConsumerId(), requestFrame.getQueueName(),
                requestFrame.getBeginOffset(), requestFrame.getMaxBatch());
        FetchMessageResponseFrame responseFrame = new FetchMessageResponseFrame(fetchResult);

        ActionFrame result = new ActionFrame();
        result.setAction(ActionType.FETCH_MESSAGE_RESPONSE);
        result.setContent(JSON.toJSONString(responseFrame));
        return result;
    }
}
