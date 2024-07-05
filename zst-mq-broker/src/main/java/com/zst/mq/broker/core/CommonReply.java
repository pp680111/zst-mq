package com.zst.mq.broker.core;

public class CommonReply {
    /**
     * 命令执行成功
     */
    public static final ActionFrame OK = new ActionFrame(ActionType.OK, null, null);

    public static final ActionFrame REQUEST_ERROR = new ActionFrame(ActionType.REQUEST_ERROR, null, null);
}
