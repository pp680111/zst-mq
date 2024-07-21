package com.zst.mq.broker.core.storage;

import com.alibaba.fastjson2.JSON;
import com.zst.mq.broker.core.Message;
import com.zst.mq.broker.core.Queue;
import com.zst.mq.broker.utils.PrivateAccessor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

        Queue.QueueMessage qm = new Queue.QueueMessage();
        qm.setMessage(message);

        System.err.println(queueStorage.write(qm));
    }

    @Test
    public void testInitBuffer_emptyData() throws IOException {
        Path filePath = Paths.get(".", "zst.dat");
        filePath.toFile().delete();
        filePath.toFile().createNewFile();

        FileChannel fileChannel = (FileChannel) Files.newByteChannel(filePath,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        ByteBuffer fileBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 100 * 1000 * 1000);

        QueueStorage queueStorage = new QueueStorage("test", ".");
        PrivateAccessor.set(queueStorage, "fileBuffer", fileBuffer);
        List<Long> offsetIndex = new ArrayList<>();
        PrivateAccessor.set(queueStorage, "offsetIndex", offsetIndex);

        PrivateAccessor.invoke(queueStorage, "initBuffer");
        Assertions.assertTrue(offsetIndex.isEmpty());
        Assertions.assertEquals(0, fileBuffer.position());
    }

    @Test
    public void testInitBuffer_hasOneData() throws IOException {
        Path filePath = Paths.get(".", "zst.dat");
        filePath.toFile().createNewFile();

        FileChannel fileChannel = (FileChannel) Files.newByteChannel(filePath,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        ByteBuffer fileBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 100 * 1000 * 1000);

        String d = "hello";
        byte[] magicBytes = PrivateAccessor.getStatic(QueueStorage.class, "MAGIC_BYTES");
        fileBuffer.put(magicBytes);
        fileBuffer.putLong(d.getBytes(StandardCharsets.UTF_8).length);
        fileBuffer.put(d.getBytes(StandardCharsets.UTF_8));

        fileBuffer.position(0);

        QueueStorage queueStorage = new QueueStorage("test", ".");
        PrivateAccessor.set(queueStorage, "fileBuffer", fileBuffer);
        List<Long> offsetIndex = new ArrayList<>();
        PrivateAccessor.set(queueStorage, "offsetIndex", offsetIndex);

        PrivateAccessor.invoke(queueStorage, "initBuffer");

        Assertions.assertFalse(offsetIndex.isEmpty());
        Assertions.assertTrue(offsetIndex.contains((long) 0));
        Assertions.assertEquals(16, fileBuffer.position());
    }

    @Test
    public void testInitBuffer_hasMultiData() throws IOException {
        Path filePath = Paths.get(".", "zst.dat");
        filePath.toFile().createNewFile();

        FileChannel fileChannel = (FileChannel) Files.newByteChannel(filePath,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        ByteBuffer fileBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 100 * 1000 * 1000);

        String d = "hello";
        byte[] magicBytes = PrivateAccessor.getStatic(QueueStorage.class, "MAGIC_BYTES");
        fileBuffer.put(magicBytes);
        fileBuffer.putLong(d.getBytes(StandardCharsets.UTF_8).length);
        fileBuffer.put(d.getBytes(StandardCharsets.UTF_8));

        fileBuffer.put(magicBytes);
        fileBuffer.putLong(d.getBytes(StandardCharsets.UTF_8).length);
        fileBuffer.put(d.getBytes(StandardCharsets.UTF_8));

        fileBuffer.put(magicBytes);
        fileBuffer.putLong(d.getBytes(StandardCharsets.UTF_8).length);
        fileBuffer.put(d.getBytes(StandardCharsets.UTF_8));

        fileBuffer.position(0);

        QueueStorage queueStorage = new QueueStorage("test", ".");
        PrivateAccessor.set(queueStorage, "fileBuffer", fileBuffer);
        List<Long> offsetIndex = new ArrayList<>();
        PrivateAccessor.set(queueStorage, "offsetIndex", offsetIndex);

        PrivateAccessor.invoke(queueStorage, "initBuffer");

        Assertions.assertEquals(3, offsetIndex.size());
        Assertions.assertTrue(offsetIndex.contains((long) 0));
        Assertions.assertTrue(offsetIndex.contains((long) 16));
        Assertions.assertTrue(offsetIndex.contains((long) 32));
        Assertions.assertEquals(48, fileBuffer.position());
    }
}
