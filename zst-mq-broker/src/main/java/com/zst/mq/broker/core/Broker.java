package com.zst.mq.broker.core;

import java.util.HashMap;
import java.util.Map;

public class Broker {
    private Map<String, Queue> queueMap;
    private Map<String, Subscription> subscriptions;
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

    private Consumer createConsumer(String consumerId) {
        Consumer consumer = new Consumer();
        consumer.setConsumerId(consumerId);
        consumer.setLastHeartbeatTime(System.currentTimeMillis());
        return consumer;
    }
}
