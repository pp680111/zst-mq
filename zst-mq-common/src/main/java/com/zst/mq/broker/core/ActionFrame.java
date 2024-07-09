package com.zst.mq.broker.core;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ActionFrame {
    private int action;
    private Map<String, String> properties;
    private String content;

    public ActionFrame(int action) {
        this.action = action;
        this.properties = new HashMap<>();
    }

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

    /**
     * 设置消费者id
     * @param consumerId
     */
    public void setConsumerId(String consumerId) {
        if (properties != null) {
            properties.put("consumerId", consumerId);
        }
    }
}
