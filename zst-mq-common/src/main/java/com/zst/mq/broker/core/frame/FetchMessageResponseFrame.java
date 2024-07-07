package com.zst.mq.broker.core.frame;

import com.zst.mq.broker.core.Message;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class FetchMessageResponseFrame {
    List<Message> messages;
}
