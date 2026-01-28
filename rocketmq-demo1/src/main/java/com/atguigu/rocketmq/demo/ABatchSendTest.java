package com.atguigu.rocketmq.demo;

import com.atguigu.rocketmq.contant.MqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ABatchSendTest {

    // 批量发送消息
    @Test
    public void testBatchProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("testBatch-producer-group");
        producer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        producer.start();
//
        System.out.println("批量发送消息开始...");

        List<Message> msgs = Arrays.asList(
                new Message("testBatchTopic",  "批量发送消息1".getBytes()),
                new Message("testBatchTopic",  "批量发送消息2".getBytes()),
                new Message("testBatchTopic",  "批量发送消息3".getBytes())
        );
        producer.send(msgs);
        System.out.println("批量发送消息成功");
        producer.shutdown();
    }

    // 批量接收消息
    @Test
    public void testBatchConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("testBatch-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        consumer.subscribe("testBatchTopic", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println("批量接收消息开始...");
                for (MessageExt messageExt : list) {
                    System.out.println(new String(messageExt.getBody()));

                }
                consumer.shutdown();
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("批量接收消息开始...");
        System.in.read();


    }
}
