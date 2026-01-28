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

import java.util.Date;
import java.util.List;

// 测试-延迟发送消息
public class ADenySendTest {

    @Test
    public void DenyProducer() throws Exception{
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("ms-producer-group");
        defaultMQProducer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        defaultMQProducer.start();
        Message message = new Message("delay-topic","我是延消息".getBytes());
//        给消息设置一个延迟时间
        message.setDelayTimeLevel(3);

//        发送延迟消息
        defaultMQProducer.send(message);
        System.out.println("发送成功");
        System.out.println("发送时间"+new Date());
//        关闭生产者
        defaultMQProducer.shutdown();


    }

    @Test
    public void DenyConsumer() throws Exception{

        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("ms-consumer-group");
        defaultMQPushConsumer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        defaultMQPushConsumer.subscribe("delay-topic","*");

        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                System.out.println("收到消息了");
                System.out.println(new String(list.get(0).getBody()));
                System.out.println("消费时间"+new Date());
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        System.out.println("消费者启动...");
        defaultMQPushConsumer.start();
        System.in.read();

    }
}
