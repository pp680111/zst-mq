package com.zst.mq.broker.core;

import com.zst.mq.broker.core.exception.BrokerException;
import com.zst.mq.broker.utils.StringUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Slf4j
public class Subscription {
    private String consumerId;
    private List<String> queues = new ArrayList<>();
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

        if (queues.contains(queueName)) {
//            throw new BrokerException(ErrorCode.DUPLICATE_SUBSCRIBE);
            log.info(MessageFormat.format("消费者重复订阅队列{0},已无视", queueName));
            return;
        }

        queues.add(queueName);
        offsetMap.put(queueName, initialOffset);
    }

    public boolean checkQueueSubscription(String queueName) {
        return queues.contains(queueName);
    }

    /**
     * 返回对订阅的队列的消费偏移量
     * @return
     */
    public Map<String, Long> getQueueOffsets() {
        return offsetMap;
    }

    /**
     * 更新对指定queue的消费偏移量
     * @param queueName
     * @param offset
     */
    public void updateQueueOffset(String queueName, Long offset) {
        if (StringUtils.isEmpty(queueName) || offset == null) {
            throw new IllegalArgumentException();
        }

        if (!queues.contains(queueName)) {
            throw new BrokerException(ErrorCode.CONSUMER_NOT_SUBSCRIBE);
        }

        offsetMap.put(queueName, offset);
    }
}
