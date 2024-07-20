package com.zst.mq.broker.core;

import com.zst.mq.broker.core.storage.QueueStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class QueueTest {
    @Test
    public void addMessage_test() {
        QueueStorage queueStorage = mock(QueueStorage.class);
        Queue queue = new Queue("test", queueStorage);
        when(queueStorage.write(any())).thenReturn(999L);

        Message message = new Message();
        message.setContent("hello");;
        Assertions.assertEquals(999L, queue.addMessage(message));

        verify(queueStorage, times(1)).write(any());
    }
}
