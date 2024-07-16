package com.zst.mq.broker.core.storage;

import java.util.HashMap;
import java.util.Map;

public class QueueStorageManager {
    private String storagePath;

    private Map<String, QueueStorage> queueStorageMap;

    public QueueStorageManager(String storagePath) {
        if (storagePath == null) {
            throw new IllegalArgumentException("storagePath is null");
        }

        this.storagePath = storagePath;
        this.queueStorageMap = new HashMap<>();
    }
    public QueueStorage getQueueStorage(String queueName) {
        if (queueStorageMap.containsKey(queueName)) {
            return queueStorageMap.get(queueName);
        }

        initQueueStorage(queueName);
        return queueStorageMap.get(queueName);
    }

    private void initQueueStorage(String queueName) {
        QueueStorage storage = new QueueStorage(this.storagePath, queueName);
        storage.init();
        queueStorageMap.put(queueName, storage);
    }
}
