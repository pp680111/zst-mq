package com.zst.mq.client.transport;

import com.zst.mq.broker.transport.TransportFrame;
import com.zst.mq.broker.utils.NamedThreadFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 负责管理客户端与服务端之间的调用结果的类
 */
public class ResponseFutureHolder {
    /**
     * future最大保留时间
     */
    private long maxWaitTimeMs = 30 * 1000L;
    private ScheduledExecutorService scheduler;
    private ConcurrentHashMap<Long, ResponseFuture> futures;
    private AtomicLong sequence;

    public ResponseFutureHolder() {
        scheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("ResponseFutureHolder-cleaner"));
        futures = new ConcurrentHashMap<>(1024);
        sequence = new AtomicLong(0);
        initCleaner();
    }

    /**
     * 注册一个回调响应future
     * @param frame
     * @return
     */
    public ResponseFuture register(TransportFrame frame) {
        if (frame == null) {
            throw new IllegalArgumentException();
        }

        long seqNo = sequence.getAndIncrement();
        frame.setSeqNo(seqNo);

        ResponseFuture future = new ResponseFuture();
        futures.put(seqNo, future);
        return future;
    }

    /**
     * 完成一个future
     * @param frame
     */
    public void completeFuture(TransportFrame frame) {
        if (frame == null) {
            throw new IllegalArgumentException();
        }
        if (frame.getSeqNo() == null) {
            return;
        }

        ResponseFuture future = futures.get(frame.getSeqNo());
        if (future == null) {
            throw new RuntimeException("未知请求序列号");
        }

        futures.remove(frame.getSeqNo());
        future.complete(frame);
    }

    /**
     * 取消一个future
     * @param seqNo
     */
    public void cancelFuture(long seqNo) {
        futures.remove(seqNo);
    }

    private void initCleaner() {
        // TODO 初始化一个任务，用来清除超时的暂存future
    }


}
