package com.atguigu.rocketmqdemo3.listener;

import com.alibaba.fastjson.JSON;
import com.atguigu.rocketmqdemo3.domain.MsgModel;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

@Component
@RocketMQMessageListener(topic = "orderlyTopic",
        consumerGroup = "orderly-consumer-group",
        consumeMode = ConsumeMode.ORDERLY)
public class OrderlyMsgListener implements RocketMQListener<MessageExt> {

    @Override
    public void onMessage(MessageExt messageExt) {
        String messageBody = new String(messageExt.getBody());

        // 检查是否为有效的JSON格式
        if (!isValidJson(messageBody)) {
            System.err.println("接收到的不是有效JSON格式: " + messageBody);
            return;
        }

        MsgModel msgModel = JSON.parseObject(messageBody, MsgModel.class);
        System.out.println("订单号：" + msgModel.getOrderSn() +
                "，用户id：" + msgModel.getUserId() +
                "，描述：" + msgModel.getDesc());
    }

    private boolean isValidJson(String jsonString) {
        try {
            JSON.parse(jsonString);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

   
}

