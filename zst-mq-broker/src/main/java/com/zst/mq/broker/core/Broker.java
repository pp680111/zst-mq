package com.zst.mq.broker.core;

import java.util.HashMap;
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
    public Subscription createSubscription(String consumerId, String queueName) {
        Subscription subscription = subscriptions.get(consumerId);
        if (subscription != null) {
            throw new RuntimeException("已存在订阅关系");
        }

        Queue queue = queueMap.get(queueName);
        if (queue == null) {
            throw new RuntimeException("不存在该队列");
        }

        Consumer consumer = consumerMap.get(consumerId);
        if (consumer == null) {
            throw new RuntimeException("当前消费者未注册");
        }

        subscription = new Subscription();
        subscription.setConsumerId(consumerId);
        subscription.setQueueName(queueName);
        subscription.setOffset(queue.currentOffset());
        subscriptions.put(consumerId, subscription);
        return subscription;
    }

    private Consumer createConsumer(String consumerId) {
        Consumer consumer = new Consumer();
        consumer.setConsumerId(consumerId);
        consumer.setLastHeartbeatTime(System.currentTimeMillis());
        return consumer;
    }
}
