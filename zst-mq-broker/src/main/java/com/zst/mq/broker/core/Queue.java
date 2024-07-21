package com.zst.mq.broker.core;

import com.zst.mq.broker.core.exception.BrokerException;
import com.zst.mq.broker.core.storage.QueueStorage;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class Queue {
    private String name;
    private QueueStorage queueStorage;


    public Queue(String name, QueueStorage queueStorage) {
        Assert.hasText(name, "name must not be null");
        Assert.notNull(queueStorage, "queueStorage must not be null");

        this.name = name;
        this.queueStorage = queueStorage;
        queueStorage.init();
    }

    /**
     * 获取队列的当前偏移量
     * @return
     */
    public long currentOffset() {
        return queueStorage.lastOffset();
    }

    /**
     * 添加消息
     * @param message
     * @return
     */
    public long addMessage(Message message) {
        synchronized (this) {
            try {

                QueueMessage queueMessage = new QueueMessage();
                queueMessage.setMessage(message);
                queueMessage.setOffset(queueStorage.currentOffset());

                this.queueStorage.write(queueMessage);

                return queueMessage.getOffset();
            } catch (Exception e) {

                throw new BrokerException(ErrorCode.MESSAGE_PUBLISH_ERROR, "add message error");
            }
        }
    }

    /**
     * 从指定的offset开始，获取指定最大数量的消息
     * @param beginOffset
     * @param batchNum
     * @return
     */
    public List<Message> fetchMessage(long beginOffset, int batchNum) {
        List<QueueMessage> queueMessages = queueStorage.fetch(beginOffset, batchNum);
        return queueMessages.stream().map(QueueMessage::getMessageForFetch).toList();
    }

    /**
     * 在Queue中使用的消息结构，对消息体进行了额外的封装
     */
    @Setter
    @Getter
    public static class QueueMessage {
        private Message message;
        private long offset;

        /**
         * 将包裹的消息复制一份返回
         *
         * 复制的目的是防止消息体的引用被拿出去之后修改了，保持消息本身的不变
         * @return
         */
        public Message getMessageForFetch() {
            Message message = new Message();
            message.setContent(this.message.getContent());

            Map<String, String> originProperties = this.message.getProperties();
            Map<String, String> newProperties = new HashMap<>();
            newProperties.put("offset", Long.toString(this.offset));
            newProperties.putAll(originProperties);
            message.setProperties(newProperties);
            return message;
        }
    }
}
