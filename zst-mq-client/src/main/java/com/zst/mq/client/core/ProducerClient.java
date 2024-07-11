package com.zst.mq.client.core;

import com.zst.mq.broker.core.Message;

import java.util.concurrent.CompletableFuture;

public class ProducerClient {
    private MQClient client;
    private String queueName;

    public ProducerClient(MQClient client, String queueName) {
        this.client = client;
        this.queueName = queueName;
    }

    public CompletableFuture<Void> publish(Message message) {
        // TODO
        return null;
    }
}
