package com.atguigu.rocketmq;

import com.atguigu.rocketmq.contant.MqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest
class RocketmqDemo1ApplicationTests {


    /**
     * 生产者-发消息
     */
    @Test
    void contextLoads() {

//        1.创建一个生成者
        DefaultMQProducer producer= new DefaultMQProducer("test-producer-group");
//        2.连接nameserver
        producer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        try {

//         3.启动
            producer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
//        4.发送消息
//        创建消息
        Message message = new Message("testTopic","我是一个简单的消息".getBytes());
        try {
//            发送消息
            SendResult send = producer.send(message);
            System.out.println(send.getSendStatus());
        } catch (Exception e) {
            e.printStackTrace();
        }
//        5.关闭消息
        producer.shutdown();

    }

    /**
     * 消费者-收消息
     *
     */

    @Test
    void receiveMsg() throws MQClientException, InterruptedException, IOException {
//        创建消费者
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("test-consumer-group");

//        连接NameServer
        defaultMQPushConsumer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);

//        订阅一个主题
        defaultMQPushConsumer.subscribe("testTopic","*");

//        设置一个监听器，监听消息
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {

                // 这个就是消费的方法（业务处理）
                System.out.println("我是消费者");
                System.out.println(new String(list.get(0).getBody()));
                System.out.println("消费上下文："+context);

//                返回值 CONSUME_SUCCESS成功，消息会从MQ出队列
//                RECONSUME_LATER  消费失败，消息会重新回到消息队列，过一会重新投递出来。
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

//        启动
        defaultMQPushConsumer.start();
        System.out.println("消费者启动");
//        一直监听不能停止
//        JVM挂起来
        System.in.read();



    }

}
