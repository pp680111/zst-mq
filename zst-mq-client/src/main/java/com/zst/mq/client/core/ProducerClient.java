package com.zst.mq.client.core;

import com.zst.mq.broker.core.Message;
import com.zst.mq.broker.utils.StringUtils;

public class ProducerClient {
    private MQClient client;

    public ProducerClient(MQClient client) {
        this.client = client;
    }

    /**
     * 发布消息
     * @param queueName
     * @param content
     * @param requireAck 是否需等待发送成功确认
     */
    public void publish(String queueName, String content, boolean requireAck) {
        if (StringUtils.isEmpty(queueName)) {
            throw new IllegalArgumentException();
        }


        Message msg = new Message();
        msg.setContent(content);
        msg.setQueueName(queueName);
        client.publishMessage(msg, requireAck);
    }
}
