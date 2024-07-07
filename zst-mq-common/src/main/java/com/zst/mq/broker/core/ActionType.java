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
     * 获取消息
     */
    int FETCH_MESSAGE = 4;
    /**
     * 拉取消费者自己的消费偏移量
     */
    int FETCH_CONSUMPTION_OFFSET = 5;
    /**
     * 提交消费偏移量
     */
    int SUBMIT_OFFSET = 6;

    // SERVER OUTBOUND ACTION
    /**
     * 发布确认
     */
    int PUBLISH_ACK = 101;
    /**
     * 操作执行成功
     */
    int OK = 102;
    int FETCH_CONSUMPTION_OFFSET_RESPONSE = 103;


    // SERVER ERROR

    /**
     * 消费者不存在
     */
    int CONSUMER_NOT_EXIST = 201;
    /**
     * 请求数据错误
     */
    int REQUEST_ERROR = 202;
}
