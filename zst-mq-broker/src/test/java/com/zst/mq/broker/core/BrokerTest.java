package com.zst.mq.broker.core;

import com.zst.mq.broker.core.storage.QueueStorageManager;
import com.zst.mq.broker.utils.PrivateAccessor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class BrokerTest {
    @Test
    public void addSubscription_hasConsumerAndQueue() {
        QueueStorageManager queueStorage = mock(QueueStorageManager.class);
        Broker broker = new Broker(queueStorage);

        broker.createQueue("zst");
        broker.updateConsumerHeartbeat("tsz");

        broker.addSubscription("tsz", "zst");

        Map<String, Subscription> subscriptions = PrivateAccessor.get(broker, "subscriptions");
        Assertions.assertTrue(subscriptions.containsKey("tsz"));

        Subscription subscription = subscriptions.get("tsz");
        List<String> queues = subscription.getQueues();
        Assertions.assertTrue(queues.contains("zst"));
        Map<String, Long> offsets = subscription.getQueueOffsets();
        Assertions.assertEquals(0L, offsets.get("zst"));
    }
}
