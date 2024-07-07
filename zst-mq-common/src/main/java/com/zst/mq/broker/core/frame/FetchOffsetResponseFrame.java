package com.zst.mq.broker.core.frame;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@AllArgsConstructor
@Getter
@Setter
public class FetchOffsetResponseFrame {
    private Map<String, Long> currentSubscriptionOffset;
}
