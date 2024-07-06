package com.zst.mq.broker.core.frame;

import com.alibaba.fastjson2.JSONObject;
import com.zst.mq.broker.core.ActionFrame;
import com.zst.mq.broker.utils.StringUtils;
import lombok.Getter;
import lombok.Setter;

/**
 * 队列订阅请求
 */
@Getter
@Setter
public class SubscribeRequestFrame {
    private String queueName;

    public static SubscribeRequestFrame fromActionFrame(ActionFrame frame) {
        if (StringUtils.isEmpty(frame.getContent())) {
            throw new IllegalArgumentException("请求数据为空");
        }

        try {
            SubscribeRequestFrame result = JSONObject.parseObject(frame.getContent(), SubscribeRequestFrame.class);
            return result;
        } catch (Exception e) {
            throw new IllegalArgumentException("解析数据格式错误");
        }
    }
}
