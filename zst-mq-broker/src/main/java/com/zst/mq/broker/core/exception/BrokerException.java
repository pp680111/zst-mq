package com.zst.mq.broker.core.exception;

public class BrokerException extends RuntimeException {
    int errCode;

    public BrokerException(int errCode) {
        super();
        this.errCode = errCode;
    }

    public BrokerException(int errCode, String message) {
        super(message);
        this.errCode = errCode;
    }

    public int getErrCode() {
        return errCode;
    }
}
