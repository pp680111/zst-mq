package com.zst.mq.broker.core;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.broker.core.exception.BrokerException;
import com.zst.mq.broker.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class Broker {
    // 在订阅或发布的队列不存在时，是否自动创建
    // 此配置因debug开启，后面需关闭
    private boolean createNotExistQueue = true;

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

    /**
     * 获取消费者对象
     * @param consumerId
     * @return
     */
    public Consumer getConsumer(String consumerId) {
        if (consumerMap.containsKey(consumerId)) {
            return consumerMap.get(consumerId);
        }

        return createConsumer(consumerId);
    }

    /**
     * 更新指定consumerId的心跳
     * @param consumerId
     */
    public void updateConsumerHeartbeat(String consumerId) {
        Consumer consumer = consumerMap.get(consumerId);
        if (consumer == null) {
            consumer = createConsumer(consumerId);
            consumerMap.put(consumerId, consumer);
        }

        consumer.setLastHeartbeatTime(System.currentTimeMillis());
    }

    /**
     * 创建队列
     * @param queueName
     */
    public void createQueue(String queueName) {
        if (StringUtils.isEmpty(queueName)) {
            throw new IllegalArgumentException();
        }

        Queue queue = queueMap.get(queueName);
        if (queue != null) {
            throw new BrokerException(ErrorCode.QUEUE_ALREADY_EXIST);
        }

        log.debug("create queue {}", queueName);

        queue = new Queue(queueName);
        queueMap.put(queueName, queue);
    }

    /**
     * 创建新的订阅关系
     * @param consumerId
     * @param queueName
     * @return
     */
    public Subscription addSubscription(String consumerId, String queueName) {
        Consumer consumer = consumerMap.get(consumerId);
        if (consumer == null) {
            throw new BrokerException(ErrorCode.CONSUMER_NOT_EXIST);
        }

        Queue queue = queueMap.get(queueName);
        if (queue == null) {
            if (createNotExistQueue) {
                createQueue(queueName);
                queue = queueMap.get(queueName);
            } else {
                throw new BrokerException(ErrorCode.QUEUE_NOT_EXIST);
            }
        }

        Subscription subscription = subscriptions.get(consumerId);
        if (subscription == null) {
            subscription = new Subscription();
            subscription.setConsumerId(consumerId);
            subscriptions.put(consumerId, subscription);

            log.debug(MessageFormat.format("create subscription from consumer {0} to {1}, initial offset = {2}",
                    consumerId, queueName, queue.currentOffset()));
        }

        subscription.addQueueSubscribe(queueName, queue.currentOffset());
        return subscription;
    }

    public Map<String, Long> queryConsumerSubscriptionOffsets(String consumerId) {
        Subscription subscription = subscriptions.get(consumerId);
        if (subscription == null) {
            throw new BrokerException(ErrorCode.CONSUMER_NOT_EXIST);
        }

        log.debug(MessageFormat.format("Consumer {0} fetch offset, result = {1}", consumerId,
                JSON.toJSONString(subscription.getQueueOffsets())));
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

        log.debug(MessageFormat.format("queue {0} add message {1}", message.getQueueName(),
                JSON.toJSONString(message)));
        queue.addMessage(message);
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
        Subscription subscription = subscriptions.get(consumerId);
        if (subscription == null) {
            throw new BrokerException(ErrorCode.CONSUMER_NOT_EXIST);
        }

        Queue queue = queueMap.get(queueName);
        if (queue == null) {
            throw new BrokerException(ErrorCode.QUEUE_NOT_EXIST);
        }

        return queue.fetchMessage(beginOffset, maxBatch);
    }

    /**
     * 提交偏移量
     * @param consumerId
     * @param queueName
     * @param offset
     */
    public void commitOffset(String consumerId, String queueName, long offset) {
        Subscription subscription = subscriptions.get(consumerId);
        if (subscription == null) {
            throw new BrokerException(ErrorCode.CONSUMER_NOT_EXIST);
        }

        if (!subscription.checkQueueSubscription(queueName)) {
            throw new BrokerException(ErrorCode.CONSUMER_NOT_SUBSCRIBE);
        }

        Queue queue = queueMap.get(queueName);
        if (queue == null) {
            throw new BrokerException(ErrorCode.QUEUE_NOT_EXIST);
        }
        long queueCurrentOffset = queue.currentOffset();
        if (offset > queueCurrentOffset) {
            throw new BrokerException(ErrorCode.INVALID_CONSUMPTION_OFFSET);
        }

        log.debug(MessageFormat.format("Consumer {0} commit queue {1} consumption offset {2}",
                consumerId, queueName, offset));
        subscription.updateQueueOffset(queueName, offset);
    }

    /**
     * 创建Consumer对象
     *
     * 加个synchronized处理一下并发请求时重复创建的问题
     * @param consumerId
     * @return
     */
    private synchronized Consumer createConsumer(String consumerId) {
        if (consumerMap.containsKey(consumerId)) {
            return consumerMap.get(consumerId);
        }

        Consumer consumer = new Consumer();
        consumer.setConsumerId(consumerId);
        consumer.setLastHeartbeatTime(System.currentTimeMillis());
        return consumer;
    }
}
