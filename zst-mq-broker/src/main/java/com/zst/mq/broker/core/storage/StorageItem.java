package com.zst.mq.broker.core.storage;

import com.alibaba.fastjson2.JSON;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@Getter
public class StorageItem {
    private static final byte[] MAGIC_BYTES = new byte[] {0x44, 0x33, 0x22};
    private long size;
    private byte[] content;

    public StorageItem(Object data) {
        if (data == null) {
            throw new IllegalArgumentException();
        }

        String jsonContent = JSON.toJSONString(data);
        content = jsonContent.getBytes(StandardCharsets.UTF_8);
        size = content.length;
    }

    private StorageItem(long size, byte[] content) {
        this.size = size;
        this.content = content;
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.put(MAGIC_BYTES);
        buffer.putLong(size);
        buffer.put(content);
    }

    public <T> T deserialize(Class<T> clazz) {
        return JSON.parseObject(content, clazz);
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

        long size = buffer.getLong(3);
        byte[] content = new byte[(int) size];
        // 跳过3byte的magic bytes和8byte的long的大小之后再开始读取
        buffer.get(content, 11, (int) size);
        return new StorageItem(size, content);
    }
}
