package com.zst.mq.broker.core.storage;

import com.zst.mq.broker.core.Message;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class QueueStorage {
    private String queueName;
    private String storagePath;
    /**
     * 默认存储文件大小上限
     */
    private long maxFileSize = 100 * 1024 * 1024;
    private MappedByteBuffer fileBuffer;

    public QueueStorage(String queueName, String storagePath) {
        this.queueName = queueName;
        this.storagePath = storagePath;
    }


    public void init() {
        try {
            Path filePath = Paths.get(storagePath, queueName, queueName + "_00.dat");
            FileChannel fileChannel = (FileChannel) Files.newByteChannel(filePath);
            this.fileBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxFileSize);
        } catch (Exception e) {
            throw new StorageException("初始化数据存储时发生错误", e);
        }
    }

    public long write(Message message) {

    }

}
