package com.zst.mq.client.core;

import com.zst.mq.broker.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ConsumerClient {
    private ConsumerProperties consumerProperties;
    /**
     * MQClient
     */
    private MQClient client;
    /**
     *
     */
    private ScheduledExecutorService executor;
    /**
     * 当前ConsumerClient订阅的队列名称
     */
    private String queueName;
    /**
     * 当前ConsumerClient消费的偏移量
     */
    private long currentOffset;
    /**
     *
     */
    private long commitedOffset;


    public ConsumerClient(MQClient client, String queueName, ConsumerProperties consumerProperties) {
        if (client == null) {
            throw new IllegalArgumentException("client 不能为空");
        }
        if (consumerProperties == null) {
            throw new IllegalArgumentException("consumerProperties 不能为空");
        }
        if (StringUtils.isEmpty(queueName)) {
            throw new IllegalArgumentException("queueName 不能为空");
        }

        this.client = client;
        this.queueName = queueName;
        this.consumerProperties = consumerProperties;
        currentOffset = 0;
        commitedOffset = 0;
    }

    public void start() {
        try {
            client.sendHeartbeat(consumerProperties.getConsumerId());
            client.subscribeQueue(consumerProperties.getConsumerId(), queueName);
            Map<String, Long> consumerOffsets = client.fetchOffset(consumerProperties.getConsumerId());
            if (consumerOffsets.containsKey(queueName)) {
                currentOffset = consumerOffsets.get(queueName);
                commitedOffset = currentOffset;
            } else {
                log.error("当前订阅的队列Broker未返回队列的offset，以默认offset=0开始");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void init() {
        scheduleHeartbeat();
    }

    private void scheduleHeartbeat() {
        executor.scheduleAtFixedRate(() -> {
            client.sendHeartbeat(consumerProperties.getConsumerId());
            log.debug("send heartbeat");
        }, 0, consumerProperties.getHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);
    }
}
