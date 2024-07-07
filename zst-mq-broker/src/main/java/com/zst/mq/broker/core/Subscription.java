package com.zst.mq.broker.core;

import com.zst.mq.broker.core.exception.BrokerException;
import com.zst.mq.broker.utils.StringUtils;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class Subscription {
    private String consumerId;
    private List<String> queueMap = new ArrayList<>();
    private Map<String, Long> offsetMap = new HashMap<>();

    /**
     * 添加新的对指定queue的订阅关系
     * @param queueName
     * @param initialOffset
     */
    public void addQueueSubscribe(String queueName, Long initialOffset) {
        if (StringUtils.isEmpty(queueName) || initialOffset == null) {
            throw new IllegalArgumentException();
        }

        if (queueName.contains(queueName)) {
            throw new BrokerException(ErrorCode.DUPLICATE_SUBSCRIBE);
        }

        queueMap.add(queueName);
        offsetMap.put(queueName, initialOffset);
    }

    /**
     * 返回对订阅的队列的消费偏移量
     * @return
     */
    public Map<String, Long> getQueueOffsets() {
        return offsetMap;
    }
}
