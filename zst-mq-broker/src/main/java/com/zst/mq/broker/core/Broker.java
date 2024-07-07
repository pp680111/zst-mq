package com.zst.mq.broker.core;

import com.zst.mq.broker.core.exception.BrokerException;
import com.zst.mq.broker.utils.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Broker {
    /**
     * key=queueName, value=Queue对象的Map
     */
    private Map<String, Queue> queueMap;
    /**
     * key=consumerId, value=Subscription对象的Map
     */
    private Map<String, Subscription> subscriptions;
    /**
     * key=consumerId, value=Consumer对象的Map
     */
    private Map<String, Consumer> consumerMap;

    public Broker() {
        queueMap = new HashMap<>();
        subscriptions = new HashMap<>();
        consumerMap = new HashMap<>();
    }

    /**
     * 获取订阅关系
     * @param consumerId
     * @return
     */
    public Subscription getSubscription(String consumerId) {
        return subscriptions.get(consumerId);
    }

    public Consumer getConsumer(String consumerId) {
        return consumerMap.get(consumerId);
    }

    /**
     * 更新指定consumerId的心跳
     * @param consumerId
     */
    public void updateConsumerHeartbeat(String consumerId) {
        Consumer consumer = consumerMap.get(consumerId);
        if (consumer != null) {
            consumer = createConsumer(consumerId);
            consumerMap.put(consumerId, consumer);
        }

        consumer.setLastHeartbeatTime(System.currentTimeMillis());
    }

    /**
     * 创建新的订阅关系
     * @param consumerId
     * @param queueName
     * @return
     */
    public Subscription addSubscription(String consumerId, String queueName) {
        Queue queue = queueMap.get(queueName);
        if (queue == null) {
            throw new BrokerException(ErrorCode.QUEUE_NOT_EXIST);
        }

        Consumer consumer = consumerMap.get(consumerId);
        if (consumer == null) {
            throw new BrokerException(ErrorCode.CONSUMER_NOT_EXIST);
        }

        Subscription subscription = subscriptions.get(consumerId);
        if (subscription == null) {
            subscription = new Subscription();
            subscription.setConsumerId(consumerId);
            subscriptions.put(consumerId, subscription);
        }

        subscription.addQueueSubscribe(queueName, queue.currentOffset());
        return subscription;
    }

    public Map<String, Long> queryConsumerSubscriptionOffsets(String consumerId) {
        Subscription subscription = subscriptions.get(consumerId);
        if (subscription == null) {
            throw new BrokerException(ErrorCode.CONSUMER_NOT_EXIST);
        }

        return subscription.getOffsetMap();
    }

    /**
     * 发布消息到指定队列中
     */
    public void publishMessages(Message message) {
        if (message == null) {
            throw new IllegalArgumentException();
        }

        Queue queue = queueMap.get(message.getQueueName());
        if (queue == null) {
            throw new BrokerException(ErrorCode.QUEUE_NOT_EXIST);
        }

        queue.addMessage(message);
    }

    private Consumer createConsumer(String consumerId) {
        Consumer consumer = new Consumer();
        consumer.setConsumerId(consumerId);
        consumer.setLastHeartbeatTime(System.currentTimeMillis());
        return consumer;
    }
}
