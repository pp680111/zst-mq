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
    @Deprecated
    int DUPLICATE_SUBSCRIBE = 10003;

    /**
     * 消费者未订阅队列
     */
    int CONSUMER_NOT_SUBSCRIBE = 10004;

    /**
     * 队列已存在
     */
    int QUEUE_ALREADY_EXIST = 10005;
    /**
     * 无效消费OFFSET
     */
    int INVALID_CONSUMPTION_OFFSET = 10006;


    default String getErrorCodeDesc(int errCode) {
        switch (errCode) {
            case CONSUMER_NOT_EXIST:
                return "消费者不存在";
            case QUEUE_NOT_EXIST:
                return "队列不存在";
            case DUPLICATE_SUBSCRIBE:
                return "消费者重复订阅";
            case CONSUMER_NOT_SUBSCRIBE:
                return "消费者未订阅队列";
            case QUEUE_ALREADY_EXIST:
                return "队列已存在";
            case INVALID_CONSUMPTION_OFFSET:
                return "无效消费OFFSET";
            default:
                return "未知错误";
        }
    }

}
