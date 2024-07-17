package com.zst.mq.broker.core.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

public class QueueStorageTest {
    @Test
    public void testInit() {
        Path p = Paths.get("d://", "xshellDownload", "tmp2");
        QueueStorage queueStorage = new QueueStorage("test", p.toString());
        queueStorage.init();
    }
}
