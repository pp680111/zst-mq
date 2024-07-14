DONE:
* 有时候会偶发的发不出消息，需要看看是什么问题
* 实现一个基于http的内存mq broker，提供消息发布，消息订阅功能



TODO:
* 实现一个基于http的mq client，使用订阅消息key，拉取消息，分发消息，消费确认，消息发送确认，消息不必持久化，分发后即焚即可
* 实现至少消费一次保证
* 优化消息在内存的对接方式，提供无限堆积的能力（直到内存炸了）
* 实现消息的持久化
* queue中消息的定期处理，防止内存占用过多
* 完善client和broker的异常处理
* 使用FileChannel的磁盘内存映射功能来做数据的高性能持久化



例子

```
FileChannel fileChannel = (FileChannel) Files.newByteChannel(Paths.get("D:\\test.txt"));
MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 1024);
```
消息在文件中的位置可以用偏移量表示，再加上消息的大小，就可以知道一份数据的位置和长度