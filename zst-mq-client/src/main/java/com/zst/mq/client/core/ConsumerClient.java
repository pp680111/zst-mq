package com.zst.mq.client.core;

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
    private long lastFetchOffset;

    public ConsumerClient(MQClient client, String queueName) {
        this.client = client;
        this.queueName = queueName;
    }

    public void start() {
        try {
            client.subscribeQueue(queueName);
            // TODO client.fetchOffset
        } catch (Exception e) {

        }
    }
}
