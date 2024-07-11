package com.zst.mq.client.core.exception;

public class BrokerException extends RuntimeException {
    private int code;
    public BrokerException(int code, String message) {
        super(message);
        this.code = code;
    }
}
