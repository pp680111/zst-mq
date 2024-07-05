package com.zst.mq.broker.core;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ActionFrame {
    private int action;
    private Map<String, String> properties;
    private String content;

    /**
     * 获取消费者id
     * @return
     */
    public String getConsumerId() {
        if (properties != null && properties.containsKey("consumerId")) {
            return properties.get("consumerId");
        }

        return null;
    }
}
