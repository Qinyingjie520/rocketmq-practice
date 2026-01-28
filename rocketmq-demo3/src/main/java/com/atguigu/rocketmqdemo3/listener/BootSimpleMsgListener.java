package com.atguigu.rocketmqdemo3.listener;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * @author qinyingjie
 */

@Component
@RocketMQMessageListener(topic="bootTestTopic",consumerGroup = "boot-test-consumer-group")
public class BootSimpleMsgListener implements RocketMQListener<MessageExt> {

    /**
     * 这个方法就是消费者方法
     * 接口范型制定了固定的类型，那么消息体就是我们的参数
     * MessageExt 类型是消息的所有内容
     *
     *
     */
    @Override
    public void onMessage(MessageExt  message) {
        System.out.println("接收到的消息是："+ new String(message.getBody()));
    }

}
