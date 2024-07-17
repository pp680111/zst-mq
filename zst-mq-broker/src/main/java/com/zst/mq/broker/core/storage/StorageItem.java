package com.zst.mq.broker.core.storage;

import lombok.Getter;

import java.nio.ByteBuffer;

@Getter
public class StorageItem {
    private static final byte[] MAGIC_BYTES = new byte[] {0x44, 0x33, 0x22};
    private long size;
    private byte[] content;

    public StorageItem(byte[] content) {
        if (content == null) {
            throw new IllegalArgumentException("content is null");
        }

        this.size = content.length;
        this.content = content;
    }

    public long writeTo(ByteBuffer buffer) {
        long beginPosition = buffer.position();
        buffer.put(MAGIC_BYTES);
        buffer.putLong(size);
        buffer.put(content);
        return beginPosition;
    }

    /**
     * 从buffer中读取数据
     * @param buffer
     * @return
     */
    public static StorageItem readFrom(ByteBuffer buffer) {
        // 测试一下空数据的时候，读取会不会卡住
        byte[] headPart = new byte[3];
        buffer.get(headPart, 0, 3);

        for (int i = 0; i < MAGIC_BYTES.length; i++) {
            if (headPart[i] != MAGIC_BYTES[i]) {
                throw new StorageException("解析数据失败");
            }
        }

        long size = buffer.getLong();
        byte[] content = new byte[(int) size];
        // 跳过3byte的magic bytes和8byte的long的大小之后再开始读取
        buffer.get(content);
        return new StorageItem(content);
    }
}
