package com.atguigu.rocketmq.demo;

import com.atguigu.rocketmq.contant.MqConstant;
import com.atguigu.rocketmq.domain.MsgModel;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class AOrderlyTest {

    private List<MsgModel> msgModels = Arrays.asList(
            new MsgModel("qwer",1,"下单"),
            new MsgModel("qwer",1,"支付"),
            new MsgModel("qwer",1,"物流"),
            new MsgModel("zxcv",2,"下单"),
            new MsgModel("zxcv",2,"支付"),
            new MsgModel("zxcv",2,"物流")
            );

    // 发送顺序消息
    @Test
    public void orderlyProducer() throws Exception {

        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("order-producer-group");
        defaultMQProducer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        defaultMQProducer.start();
        // 发送顺序消息，发送时要确保有序，确保同一个key的消息发送到同一个队列中
        for (MsgModel msgModel : msgModels) {
            Message message = new Message("orderlyTopic",msgModel.toString().getBytes() );
            // 发相同的订单号去相同的队列
            defaultMQProducer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    // 选择队列
                    int index = Math.abs(msgModel.getOrderSn().hashCode()) % list.size();
                    return list.get(index);

                }
            },msgModel.getOrderSn());
        }
        defaultMQProducer.shutdown();
        System.out.println("发送完毕时间:"+new Date());
        System.out.println("发送顺序消息成功");
    }
    // 接收顺序消息
    @Test
    public void orderlyConsumer() throws Exception {

        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("order-consumer-group");
        defaultMQPushConsumer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        defaultMQPushConsumer.subscribe("orderlyTopic", "*");

        // new MessageListenerConcurrently 是以并发模式,多线程去消费消息
//        new MessageListenerOrderly  顺序模式，单线程去消费消息 每个队列对应一个线程
        defaultMQPushConsumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                System.out.println("线程id = "+Thread.currentThread().getId());
                System.out.println(new String(list.get(0).getBody()));
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        defaultMQPushConsumer.start();
        System.out.println("接收顺序消息成功");
        System.out.println("接收顺序消息时间:"+new Date());
        System.in.read();
    }
}
