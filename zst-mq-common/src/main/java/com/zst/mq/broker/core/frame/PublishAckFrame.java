package com.zst.mq.broker.core.frame;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 发布消息时的确认回调响应
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class PublishAckFrame {
    /**
     * 消息ID
     */
    private String messageId;
    /**
     * 是否成功
     */
    private boolean success;
    /**
     * 失败原因
     */
    private String cause;
}
