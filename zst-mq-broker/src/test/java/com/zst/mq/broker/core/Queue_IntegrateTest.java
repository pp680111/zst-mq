package com.zst.mq.broker.core;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.broker.core.storage.QueueStorage;
import com.zst.mq.broker.core.storage.QueueStorageManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Queue_IntegrateTest {
    @Test
    public void addMessage_test() {
        QueueStorageManager queueStorageManager = new QueueStorageManager(".");
        QueueStorage queueStorage = queueStorageManager.getQueueStorage("test");

        Queue queue = new Queue("test", queueStorage);

        Message msg1 = new Message();
        msg1.setContent("hello");
        Map<String, String> p1 = new HashMap<>();
        p1.put("ke", "v1");
        msg1.setProperties(p1);

        System.err.println(queue.addMessage(msg1));

        Message msg2 = new Message();
        msg2.setContent("world");
        Map<String, String> p2 = new HashMap<>();
        p2.put("ke", "v2");
        msg2.setProperties(p2);
        System.err.println(queue.addMessage(msg2));
    }

    @Test
    public void fetchMessage_test() {
        QueueStorageManager queueStorageManager = new QueueStorageManager(".");
        QueueStorage queueStorage = queueStorageManager.getQueueStorage("test");

        Queue queue = new Queue("test", queueStorage);

        List<Message> messages = queue.fetchMessage(0, 10);
        Assertions.assertNotNull(messages);
        Assertions.assertEquals(2, messages.size());
        System.err.println(JSON.toJSONString(messages));
    }
}
