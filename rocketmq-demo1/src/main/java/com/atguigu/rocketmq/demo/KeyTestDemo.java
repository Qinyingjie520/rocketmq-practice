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

import java.util.List;
import java.util.UUID;

public class KeyTestDemo {

    @Test
    public void keyProducer() throws Exception {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("key-producer-group");
        defaultMQProducer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        defaultMQProducer.start();
        String key = UUID.randomUUID().toString();
        System.out.println(key);
        Message message = new Message("keyTopic","vip1",key,"我是vip1的文章".getBytes());
        defaultMQProducer.send(message);
        System.out.println(Thread.currentThread().getName()+"发送成功---");
        defaultMQProducer.shutdown();

    }

    @Test
    public void keyConsumer() throws Exception {

        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("key-consumer-group");
        defaultMQPushConsumer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        defaultMQPushConsumer.subscribe("keyTopic","*");
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println("我是vip1的消费者，我正在消费消息:"+new String(list.get(0).getBody()));
                System.out.println("我们业务的标识："+list.get(0).getKeys());
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        defaultMQPushConsumer.start();
        System.out.println("消费者启动成功");

        System.in.read();

    }
}
