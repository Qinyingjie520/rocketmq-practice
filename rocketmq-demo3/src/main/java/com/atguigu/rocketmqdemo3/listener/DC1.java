package com.atguigu.rocketmqdemo3.listener;

import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

@Component
@RocketMQMessageListener(topic = "modeTopic",
        consumerGroup = "mode-consumer-group-a",
        messageModel = MessageModel.CLUSTERING
//        集群模式，负载均衡
)
public class DC1 implements RocketMQListener<String> {
    @Override
    public void onMessage(String s) {
        System.out.println("我是mode-consumer-group-a组的第1个消费者：" + s);
    }
}
