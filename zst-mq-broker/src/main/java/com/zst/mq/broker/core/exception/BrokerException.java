package com.zst.mq.broker.core.exception;

import lombok.Getter;

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
