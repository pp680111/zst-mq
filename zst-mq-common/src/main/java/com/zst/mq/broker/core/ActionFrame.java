package com.zst.mq.broker.core;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ActionFrame {
    private int action;
    private String content;
}
