TODO:
* 实现一个基于http的内存mq broker，提供消息发布，消息订阅功能
* 实现一个基于http的mq client，使用订阅消息key，拉取消息，分发消息，消费确认，消息发送确认，消息不必持久化，分发后即焚即可
* 实现至少消费一次保证
* 优化消息在内存的对接方式，提供无限堆积的能力（直到内存炸了）
* 实现消息的持久化
* queue中消息的定期处理，防止内存占用过多
* 完善client和broker的异常处理
* 有时候会偶发的发不出消息，需要看看是什么问题


consumer和queue的订阅关系可以抽象为Subscription类，在里面记录QueueId或topicId、consumerId和offset，offset也能用来区分不同consumer的
消费进度
