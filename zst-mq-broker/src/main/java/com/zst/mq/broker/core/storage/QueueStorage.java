package com.zst.mq.broker.core.storage;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.broker.core.Message;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

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
            if (!Files.exists(filePath)) {
                File file = filePath.toFile();
                file.getParentFile().mkdirs();
                file.createNewFile();
            }

            FileChannel fileChannel = (FileChannel) Files.newByteChannel(filePath,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            this.fileBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxFileSize);
        } catch (Exception e) {
            throw new StorageException("初始化数据存储时发生错误", e);
        }
    }

    /**
     * 将消息写入到文件存储中，返回消息的偏移量
     * @param message
     * @return
     */
    public long write(Message message) {
        if (message == null) {
            throw new IllegalArgumentException();
        }

        String content = JSON.toJSONString(message);
        StorageItem storageItem = new StorageItem(content.getBytes(StandardCharsets.UTF_8));
        return storageItem.writeTo(this.fileBuffer);
    }

    /**
     * 从文件存储中获取位于指定位置的消息数据
     * @param offset
     * @return
     */
    public Message get(long offset) {
        try {
            ByteBuffer roBuffer = this.fileBuffer.asReadOnlyBuffer();
            roBuffer.position((int) offset);
            StorageItem storageItem = StorageItem.readFrom(roBuffer);
            return JSON.parseObject(new String(storageItem.getContent(), StandardCharsets.UTF_8), Message.class);
        } catch (Exception e) {
            throw new StorageException("读取消息数据时发生错误", e);
        }
    }

    /**
     * 初始化buffer，恢复position到最后的空白位置
     */
    private void initBuffer() {

    }
}
