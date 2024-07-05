package com.zst.mq.broker.core;

public interface ActionType {
    // SERVER INBOUND ACTION
    /**
     * 心跳包
     */
    int HEARTBEAT = 0;
    /**
     * 订阅队列
     */
    int SUBSCRIBE_QUEUE = 1;
    /**
     * 取消订阅队列
     */
    int UNSUBSCRIBE_QUEUE = 2;
    /**
     * 发布消息
     */
    int PUBLISH_MESSAGE = 3;
    /**
     * 拉取消息
     */
    int FETCH_MESSAGE = 5;
    /**
     * 提交消费偏移量
     */
    int SUBMIT_OFFSET = 6;

    // SERVER OUTBOUND ACTION
    /**
     * 发布确认
     */
    int PUBLISH_ACK = 101;
}
