package com.atguigu.rocketmq.demo;

import com.atguigu.rocketmq.contant.MqConstant;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;

/**
 * 测试-发送单向消息
 */
public class AOneWayTest {

    @Test
    public void oneWayProducer() throws Exception{
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("oneway-producer-group");
        defaultMQProducer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        defaultMQProducer.start();
        Message message = new Message("onewaytopic","日志xxx".getBytes());
        defaultMQProducer.sendOneway(message);
        System.out.println("发送成功!");
        defaultMQProducer.shutdown();

    }
}
