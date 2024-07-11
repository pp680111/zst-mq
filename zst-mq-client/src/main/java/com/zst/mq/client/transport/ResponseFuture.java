package com.zst.mq.client.transport;

import com.zst.mq.broker.transport.TransportFrame;

import java.util.concurrent.CompletableFuture;

public class ResponseFuture extends CompletableFuture<TransportFrame> {
    private long seqNo;
    private long startTime;

    public ResponseFuture() {
        this.startTime = System.currentTimeMillis();
    }
}