package com.zst.mq.broker.core.storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class StorageItemTest {
    @Test
    public void testWriteTo() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        String data = "hello";
        byte[] content = data.getBytes();
        StorageItem storageItem = new StorageItem(content);
        storageItem.writeTo(byteBuffer);

        byteBuffer.flip();
        Assertions.assertEquals((byte) 0x44, byteBuffer.get());
        Assertions.assertEquals((byte) 0x33, byteBuffer.get());
        Assertions.assertEquals((byte) 0x22, byteBuffer.get());

        Assertions.assertEquals(content.length, byteBuffer.getLong());

        byte[] result = new byte[content.length];
        byteBuffer.get(result);
        Assertions.assertEquals("hello", new String(result));
    }

    @Test
    public void testReadFromByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        String data = "hello";
        byte[] content = data.getBytes();
        byteBuffer.put((byte) 0x44);
        byteBuffer.put((byte) 0x33);
        byteBuffer.put((byte) 0x22);
        byteBuffer.putLong(content.length);
        byteBuffer.put(content);

        byteBuffer.flip();
        StorageItem storageItem = StorageItem.readFrom(byteBuffer);
        Assertions.assertEquals("hello", new String(storageItem.getContent()));
    }
}
