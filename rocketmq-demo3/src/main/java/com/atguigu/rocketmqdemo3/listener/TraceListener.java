package com.atguigu.rocketmqdemo3.listener;

import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

@Component
@RocketMQMessageListener(topic = "traceTopic",
        consumerGroup = "trace-consumer-group",
        consumeThreadNumber = 40,
        consumeMode = ConsumeMode.CONCURRENTLY,
        enableMsgTrace = true
)
public class TraceListener implements RocketMQListener<String> {
    @Override
    public void onMessage(String message) {
        System.out.println("接收到的消息：" + message);
    }
}
