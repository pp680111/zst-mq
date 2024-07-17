package com.zst.mq.broker.core.storage;

import com.zst.mq.broker.core.Message;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class QueueStorageTest {
    @Test
    public void testInit() {
        Path p = Paths.get(".");
        QueueStorage queueStorage = new QueueStorage("test", p.toString());
        queueStorage.init();
    }

    @Test
    public void testWrite() {
        Path p = Paths.get(".");
        QueueStorage queueStorage = new QueueStorage("test", p.toString());
        queueStorage.init();

        Message message = new Message();
        message.setContent("hello");
        message.setMessageId("msgId");
        message.setQueueName("test");
        Map<String, String> prop = new HashMap<>();
        prop.put("h", "s");
        message.setProperties(prop);

        System.err.println(queueStorage.write(message));
    }
}
