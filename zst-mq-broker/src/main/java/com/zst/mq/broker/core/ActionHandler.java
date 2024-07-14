package com.zst.mq.broker.core;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.broker.core.exception.BrokerException;
import com.zst.mq.broker.core.frame.FetchMessageRequestFrame;
import com.zst.mq.broker.core.frame.FetchMessageResponseFrame;
import com.zst.mq.broker.core.frame.FetchOffsetResponseFrame;
import com.zst.mq.broker.core.frame.OffsetCommitRequestFrame;
import com.zst.mq.broker.core.frame.PublishAckFrame;
import com.zst.mq.broker.core.frame.PublishMessageFrame;
import com.zst.mq.broker.core.frame.SubscribeRequestFrame;
import com.zst.mq.broker.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

@Slf4j
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
                return handleSubscribeQueue(frame);
            case ActionType.UNSUBSCRIBE_QUEUE:
                break;
            case ActionType.PUBLISH_MESSAGE:
                return handlePublishMessage(frame);
            case ActionType.FETCH_CONSUMPTION_OFFSET:
                return handleFetchConsumptionOffset(frame);
            case ActionType.FETCH_MESSAGE:
                return handleFetchMessage(frame);
            case ActionType.SUBMIT_OFFSET:
                return handleSubmitOffset(frame);
            default:
                return CommonReply.UNKNOWN_ACTION;
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
            log.debug(MessageFormat.format("received consumer {0} heartbeat request", consumerId));

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
            publishAckFrame.setCause(ErrorCode.getErrorCodeDesc(e.getErrCode()));
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

    /**
     * 处理提交offset请求
     * @param frame
     * @return
     */
    private ActionFrame handleSubmitOffset(ActionFrame frame) {
        OffsetCommitRequestFrame requestFrame = OffsetCommitRequestFrame.fromActionFrame(frame);

        if (StringUtils.isEmpty(requestFrame.getQueueName()) || requestFrame.getOffset() == null
                || StringUtils.isEmpty(frame.getConsumerId())) {
            return CommonReply.REQUEST_ERROR;
        }

        broker.commitOffset(frame.getConsumerId(), requestFrame.getQueueName(), requestFrame.getOffset());

        return CommonReply.OK;
    }
}
