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


public class TagTestDemo {


    @Test
    public void tagProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("tag-producer-group");
        producer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        producer.start();
        Message message1 = new Message("tagTopic", "vip1", "我是vip1的文章".getBytes());
        Message message2 = new Message("tagTopic", "vip2", "我是vip2的文章".getBytes());
        producer.send(message1);
        producer.send(message2);
        System.out.println("发送成功！");
        producer.shutdown();

    }
//    订阅关系一致性:消费vip1的消费者
    @Test
    public void tagConsumer1() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tag-consumer-group-a");
        consumer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        consumer.subscribe("tagTopic", "vip1");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                byte[] body = list.get(0).getBody();
                System.out.println("我是VIP1的消费者，我正在消费消息:"+new String(body));

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }

    //    订阅关系一致性:消费vip2的消费者
    @Test
    public void tagConsumer2() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tag-consumer-group-b");
        consumer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        consumer.subscribe("tagTopic", "vip1||vip2");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                byte[] body = list.get(0).getBody();
                System.out.println("我是VIP2的消费者，我正在消费消息:"+new String(body));

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }
}
