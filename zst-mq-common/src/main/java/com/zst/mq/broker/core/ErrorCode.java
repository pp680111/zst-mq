package com.zst.mq.broker.core;

public interface ErrorCode {
    /**
     * 消费者未注册
     */
    int CONSUMER_NOT_EXIST = 10001;
    /**
     * 队列不存在
     */
    int QUEUE_NOT_EXIST = 10002;
    /**
     * 消费者重复订阅
     */
    int DUPLICATE_SUBSCRIBE = 10003;

    default String getErrorCodeDesc(int errCode) {
        switch (errCode) {
            case CONSUMER_NOT_EXIST:
                return "消费者不存在";
            case QUEUE_NOT_EXIST:
                return "队列不存在";
            case DUPLICATE_SUBSCRIBE:
                return "消费者重复订阅";
            default:
                return "未知错误";
        }
    }

}
