package com.zst.mq.broker.core;

import com.zst.mq.broker.core.frame.SubscribeRequestFrame;
import com.zst.mq.broker.utils.StringUtils;

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
                break;
            case ActionType.UNSUBSCRIBE_QUEUE:
                break;
            case ActionType.PUBLISH_MESSAGE:
                break;
            case ActionType.FETCH_MESSAGE:
                break;
            case ActionType.SUBMIT_OFFSET:
                break;
        }

        return null;
    }

    private ActionFrame handleHeartbeat(ActionFrame frame) {
        String consumerId = frame.getConsumerId();
        if (StringUtils.isNotEmpty(consumerId)) {
            broker.updateConsumerHeartbeat(consumerId);
            return CommonReply.OK;
        }

        return CommonReply.REQUEST_ERROR;
    }

    private ActionFrame handleSubscribeQueue(ActionFrame frame) {
        String consumerId = frame.getConsumerId();
        SubscribeRequestFrame request = SubscribeRequestFrame.fromActionFrame(frame);

        Subscription subscription = broker.createSubscription(consumerId, request.getQueueName());
        // TODO 构建返回结果
    }
}
