package com.zst.mq.client.core;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class ConsumerClient {
    /**
     * MQClient
     */
    private MQClient client;
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


    public ConsumerClient(MQClient client, String queueName) {
        this.client = client;
        this.queueName = queueName;
        currentOffset = 0;
        commitedOffset = 0;
    }

    public void start() {
        try {
            client.subscribeQueue(queueName);
            Map<String, Long> consumerOffsets = client.fetchOffset();
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
}
