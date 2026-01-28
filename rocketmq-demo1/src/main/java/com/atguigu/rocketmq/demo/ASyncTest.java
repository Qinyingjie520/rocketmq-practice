package com.atguigu.rocketmq.demo;

import com.atguigu.rocketmq.contant.MqConstant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;

// 测试-发送异步消息
public class ASyncTest {


    @Test
    public void asyncProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("async-producer-group");
        producer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        producer.start();
        Message message = new Message("async-topic", "我是一个异步消息".getBytes());
        producer.send(message, new SendCallback() {

//            成功之后的回调
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println(sendResult.getSendStatus());
                System.out.println("发送成功！");
            }
//            异常的回调
            @Override
            public void onException(Throwable throwable) {
                System.out.println(throwable.getMessage());
                System.out.println("发送失败！");
            }
        });
        System.out.println("我先执行");
//        挂起来
        System.in.read();


    }
}
