package com.zst.mq.broker.bootstrap;

import com.zst.mq.broker.core.ActionHandler;
import com.zst.mq.broker.core.Broker;
import com.zst.mq.broker.core.BrokerProperties;
import com.zst.mq.broker.core.storage.QueueStorageManager;
import com.zst.mq.broker.transport.NettyTransport;
import com.zst.mq.broker.transport.NettyTransportProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.Optional;

@Configuration
public class ContextConfiguration {
    @Bean
    public BrokerProperties brokerProperties(Environment environment) {
        BrokerProperties properties = new BrokerProperties();
        Optional.ofNullable(environment.getProperty("broker.storagePath"))
                .ifPresent(properties::setStoragePath);
        return properties;
    }

    @Bean
    public QueueStorageManager queueStorageManager(BrokerProperties brokerProperties) {
        return new QueueStorageManager(brokerProperties.getStoragePath());
    }

    @Bean
    public Broker broker(QueueStorageManager queueStorageManager) {
        return new Broker(queueStorageManager);
    }

    @Bean
    public ActionHandler actionHandler(Broker broker) {
        return new ActionHandler(broker);
    }

    @Bean
    public NettyTransportProperties nettyTransportProperties(Environment environment) {
        NettyTransportProperties properties = new NettyTransportProperties();
        Optional.ofNullable(environment.getProperty("transport.netty.port", Integer.class))
                .ifPresent(properties::setPort);
        Optional.ofNullable(environment.getProperty("transport.netty.bizWorkerNum", Integer.class))
                .ifPresent(properties::setBizWorkerNum);
        Optional.ofNullable(environment.getProperty("transport.netty.ioWorkerNum", Integer.class))
                .ifPresent(properties::setIoWorkerNum);
        return properties;
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public NettyTransport nettyTransport(NettyTransportProperties properties,
                                         ActionHandler actionHandler) {
        return new NettyTransport(properties, actionHandler);
    }

}
