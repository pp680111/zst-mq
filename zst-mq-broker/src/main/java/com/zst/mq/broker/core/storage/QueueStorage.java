package com.zst.mq.broker.core.storage;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.broker.core.Queue;
import org.springframework.util.Assert;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueueStorage implements Closeable {
    private static final byte[] MAGIC_BYTES = new byte[] {0x44, 0x33, 0x22};

    private String queueName;
    private String storagePath;
    private List<Long> offsetIndex;
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

            this.offsetIndex = new ArrayList<>();
            initBuffer();
        } catch (Exception e) {
            throw new StorageException("初始化数据存储时发生错误", e);
        }
    }

    @Override
    public void close() throws IOException {
        offsetIndex = null;
        this.fileBuffer = null;
    }

    /**
     * 将消息写入到文件存储中，返回消息的偏移量
     * @param message
     * @return
     */
    public long write(Queue.QueueMessage message) {
        if (message == null) {
            throw new IllegalArgumentException();
        }

        long currentIndex = this.fileBuffer.position();

        String content = JSON.toJSONString(message);
        this.fileBuffer.put(MAGIC_BYTES);
        this.fileBuffer.putLong(content.length());
        this.fileBuffer.put(content.getBytes(StandardCharsets.UTF_8));

        this.offsetIndex.add(currentIndex);
        return currentIndex;
    }

    /**
     * 从文件存储中获取位于指定位置的消息数据
     * @param offset
     * @return
     */
    public Queue.QueueMessage get(long offset) {
        if (!offsetIndex.contains(offset)) {
            throw new StorageException("偏移量参数值错误");
        }

        try {
            ByteBuffer roBuffer = this.fileBuffer.asReadOnlyBuffer();
            roBuffer.position((int) offset);

            byte[] headPart = new byte[3];
            roBuffer.get(headPart, 0, 3);

            for (int i = 0; i < MAGIC_BYTES.length; i++) {
                if (headPart[i] != MAGIC_BYTES[i]) {
                    throw new StorageException("解析数据失败");
                }
            }
            long size = roBuffer.getLong();
            byte[] content = new byte[(int) size];
            roBuffer.get(content);

            return JSON.parseObject(new String(content, StandardCharsets.UTF_8), Queue.QueueMessage.class);
        } catch (Exception e) {
            throw new StorageException("读取消息数据时发生错误", e);
        }
    }

    /**
     * 批量获取消息
     * @param beginOffset
     * @param batchNum
     * @return
     */
    public List<Queue.QueueMessage> fetch(long beginOffset, int batchNum) {
        Assert.isTrue(beginOffset >= 0, "偏移量参数值错误");

        if (offsetIndex.isEmpty()) {
            return Collections.emptyList();
        }

        List<Queue.QueueMessage> result = new ArrayList<>();
        for (Long offset : offsetIndex) {
            if (offset >= beginOffset) {
                result.add(get(offset));

                if (result.size() >= batchNum) {
                    break;
                }
            }
        }

        return result;
    }

    /**
     * 获取当前最后一条消息的offset
     * @return
     */
    public long lastOffset() {
        if (offsetIndex.isEmpty()) {
            return 0L;
        }
        return this.offsetIndex.get(this.offsetIndex.size() - 1);
    }

    /**
     * 获取当前偏移量
     * @return
     */
    public long currentOffset() {
        return this.fileBuffer.position();
    }

    /**
     * 初始化buffer，恢复position到最后的空白位置
     */
    private void initBuffer() {
        while (true) {
            long i = this.fileBuffer.position();

            byte[] headPart = new byte[3];
            this.fileBuffer.get(headPart);

            if (headPart[0] != MAGIC_BYTES[0]
                    && headPart[1] != MAGIC_BYTES[1]
                    && headPart[2] != MAGIC_BYTES[2]) {
                // 如果读不到magic_bytes，就回滚一下index
                this.fileBuffer.position((int) i);
                break;
            }

            offsetIndex.add(i);

            long size = this.fileBuffer.getLong();
            this.fileBuffer.position((int) (i + 3 + 8 + size));
        }
    }
}
